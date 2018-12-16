#!/usr/bin/env python

import random
import sqlite3

class MarkovPrefixSql(object):

    SCHEMA_VER = "0.0.1"
    SCHEMA_INIT = [
        """CREATE TABLE IF NOT EXISTS metadata (
            key TEXT NOT NULL,
            value TEXT,
            PRIMARY KEY (key) ON CONFLICT REPLACE
        );""",
        """CREATE TABLE IF NOT EXISTS prefixes ( 
            prefix_id INTEGER PRIMARY KEY,
            prefix TEXT NOT NULL,
            num_seen INTEGER NOT NULL,
            UNIQUE (prefix)
        );""",
        """CREATE TABLE IF NOT EXISTS leaves (
            leaf_id INTEGER PRIMARY KEY,
            prefix_id INT NOT NULL,
            suffix TEXT,
            num_seen INT NOT NULL,
            FOREIGN KEY (prefix_id) REFERENCES prefixes(prefix_id)
        );""",
        """CREATE INDEX IF NOT EXISTS leaf_prefix ON leaves(prefix_id);""",
        """CREATE TABLE IF NOT EXISTS leaf_labels (
            leaf_label_id INTEGER PRIMARY KEY,
            leaf_id INTEGER NOT NULL,
            label TEXT,
            UNIQUE (leaf_id, label) ON CONFLICT IGNORE,
            FOREIGN KEY (leaf_id) REFERENCES leaves(leaf_id)
        );""",
        """CREATE INDEX IF NOT EXISTS leaf_label ON leaf_labels(leaf_id);""",
    ]

    def __init__(self, max=4, min=None, seperator=None, dbfile=None):
        self._max = max
        if min is not None and min != max:
            raise ValueError('Minimum must equal maximum for prefix chains')
        self._min = max
        self._seperator = seperator
        self.initDB(dbfile)

    def initDB(self, filename):
        if filename is None:
            filename = ':memory:'
        self._prefixdb = sqlite3.connect(filename)
        self._cursor = self._prefixdb.cursor()
        self._cursor.execute('PRAGMA journal_mode=wal')
        for statement in self.SCHEMA_INIT:
            try:
                self._cursor.execute(statement);
            except sqlite3.OperationalError as e:
                print('Statement "{}" gave error {}'.format(statement, str(e)))
        self.initMeta()

    def initMeta(self):
        dbschema_version = self._getMeta('schema_version')
        if dbschema_version is None:
            self._setMeta('schema_version', self.SCHEMA_VER)
        elif dbschema_version != self.SCHEMA_VER:
            raise ValueError('This database created with schema version {}, needs {}'.format(dbschema_version, self.SCHEMA_VER))
            
        dbmax = self._getMeta('max_chain_length')
        if dbmax is None:
            self._setMeta('max_chain_length', self._max)
        elif int(dbmax) != self._max:
            raise ValueError('max for db already set to {}, not {}'.format(dbmax, max))
        sep = self._getMeta('seperator')
        if sep is None:
            if self._seperator is None:
                self._seperator = ' '
            self._setMeta('seperator', self._seperator)
        else: 
            if self._seperator is None:
                self._seperator = sep
            elif sep != self._seperator:
                raise ValueError('seperator does not match what db was configured with')

    def _getMeta(self, key):
        results = self._cursor.execute("SELECT value FROM metadata WHERE key = ?;", [key,])
        for result in results:
            return result[0]
        return None

    def _setMeta(self, key, value):
        self._cursor.execute("INSERT INTO metadata(key, value) VALUES(?, ?);", [key, str(value)])

    def Update(self, seq, label=None):
        subseq = seq[:self._max-1]

        for element in seq[self._max-1:]:
            subseq.append(element)
            self._updateTuple(subseq, label)
            subseq = subseq[1:] # pop off the first element

        self._updateTuple(subseq, label)
        self._prefixdb.commit()

    def _updateTuple(self, t, l):
        prefix = t[0:self._max-1]
        if len(t) >= self._max:
            leaf = t[self._max-1]
        else:
            leaf = self._seperator

        prefix_id = self._getAndIncPrefixId(prefix)
        self._getAndIncLeafId(prefix_id, leaf, l)

    def _getAndIncPrefixId(self, prefix):
        prefix_id = self._getPrefixId(prefix)
        if prefix_id is not None:
            self._cursor.execute("""
                UPDATE prefixes SET num_seen = num_seen + 1
                    WHERE prefix_id = ?; """, [prefix_id,]);
        else:
            prefix_str = self._seperator.join(prefix)
            self._cursor.execute("""
                INSERT INTO prefixes(prefix, num_seen) VALUES (?, ?);""",
                [prefix_str, 1])
            prefix_id = self._cursor.lastrowid
            self._last_prefix_count = 1;
        return prefix_id

    def _getPrefixId(self, prefix):
        prefix_str = self._seperator.join(prefix)
        results = self._cursor.execute("""
            SELECT prefix_id, num_seen FROM prefixes WHERE prefix = ?;""",
            [prefix_str, ])
        for prefix_id, count in results:
            self._last_prefix_count = count
            return prefix_id
        self._last_prefix_count = 0
        return None

    def _getAndIncLeafId(self, prefix_id, suffix, label):
        leaf_id = self._getLeafId(prefix_id, suffix)
        if leaf_id is not None:
            self._cursor.execute("""
                UPDATE leaves SET num_seen = num_seen + 1
                    WHERE leaf_id = ?;""", [leaf_id, ])
        else:
            self._cursor.execute("""
                INSERT INTO leaves(prefix_id, suffix, num_seen)
                    VALUES(?, ?, ?);""", [prefix_id, suffix, 1])
            leaf_id = self._cursor.lastrowid
            self._last_leaf_count = 1
        if label:
            self._updateLeafLabel(leaf_id, label)
        return leaf_id

    def _getLeafId(self, prefix_id, suffix):
        results = self._cursor.execute("""
            SELECT leaf_id, num_seen FROM leaves
                WHERE prefix_id = ? and suffix = ?;""", [prefix_id, suffix])
        for leaf_id, count in results:
            self._last_leaf_count = count
            return leaf_id
        self._last_leaf_count = 0
        return None

    def _updateLeafLabel(self, leaf_id, label):
        try:
            self._cursor.execute("""
                INSERT INTO leaf_labels(leaf_id, label) 
                    VALUES(?, ?);""", [leaf_id, label])
        except sqlite3.OperationalError:
            pass

    def GetRandomTuple(self, seed, depth=None, labelset=None):
        if depth is not None and depth != self._max:
            raise ValueError('depth!=max not supported in prefix chains')
        prefix = seed[:self._max-1]
        if len(prefix) < self._max - 1:
            prefix_id, prefix = self._getPrefixIdLike(prefix)
        else:
            prefix_id = self._getPrefixId(prefix)

        if prefix_id is None:
            return None

        leaf_id, leaf = self._getRandomLeaf(prefix_id, labelset)

        return tuple(prefix) + (leaf,)

    def _getPrefixIdLike(self, prefix):
        prefix_search_str = self._seperator.join(prefix) + self._seperator + '%';
        results = self._cursor.execute("""
            SELECT prefix_id, prefix, num_seen FROM prefixes
                WHERE prefix like ?;""", [prefix_search_str,])
        prefix_map = {}
        total_count = 0
        for prefix_id, prefix_str, count in results:
            total_count += count
            prefix_map[(prefix_id, prefix_str)] = count
        if total_count == 0:
            return None, None
        target = random.randint(0, total_count-1)
        for key, this_count in prefix_map.items():
            if target < this_count:
                self._last_prefix_count = this_count
                return key[0], self._tokenize(key[1])
            else:
                target -= this_count
        assert False, 'This should\'t happen'

    def _tokenize(self, string):
        if self._seperator == ' ':
            return list(string.split())
        elif self._seperator:
            return list(string.split(self.seperator))
        else:
            return list(string)

    def _getRandomLeaf(self, prefix_id, labelset):
        assert self._last_prefix_count > 0, 'Didn\'t we find anything?'
        target = random.randint(0, self._last_prefix_count - 1)
        results = self._cursor.execute("""
            SELECT leaf_id, suffix, num_seen FROM leaves
                WHERE prefix_id = ?;""", [prefix_id,])
        for leaf_id, suffix, count in results:
            if target < count:
                self._updateLabelsFromLeaf(leaf_id, labelset)
                return leaf_id, suffix
            else:
                target -= count
        return None, None

    def _updateLabelsFromLeaf(self, leaf_id, labelset):
        if labelset is None:
            return

        results = self._cursor.execute("SELECT label FROM leaf_labels WHERE leaf_id = ?;", [leaf_id,])
        for row in results:
            labelset.add(row[0])


if __name__ == '__main__':
    import sys
    chain = MarkovPrefixSql(max=int(sys.argv[1]), dbfile=sys.argv[2])
    seq = sys.argv[3:]
    if seq:
        chain.Update(seq, 'argv')
    t = chain.GetRandomTuple(sys.argv[3:5])
    print(repr(t))

