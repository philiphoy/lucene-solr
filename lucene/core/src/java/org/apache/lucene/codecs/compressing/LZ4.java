package org.apache.lucene.codecs.compressing;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.packed.PackedInts;

/**
 * LZ4 compression and decompression routines.
 *
 * http://code.google.com/p/lz4/
 * http://fastcompression.blogspot.fr/p/lz4.html
 */
class LZ4 {

  private LZ4() {}

  static final int MEMORY_USAGE = 14;
  static final int MIN_MATCH = 4; // minimum length of a match
  static final int MAX_DISTANCE = 1 << 16; // maximum distance of a reference
  static final int LAST_LITERALS = 5; // the last 5 bytes must be encoded as literals
  static final int HASH_LOG_HC = 15; // log size of the dictionary for compressHC
  static final int HASH_TABLE_SIZE_HC = 1 << HASH_LOG_HC;
  static final int OPTIMAL_ML = 0x0F + 4 - 1; // match length that doesn't require an additional byte


  private static int hash(int i, int hashBits) {
    return (i * -1640531535) >>> (32 - hashBits);
  }

  private static int hashHC(int i) {
    return hash(i, HASH_LOG_HC);
  }

  private static int readInt(byte[] buf, int i) {
    return ((buf[i] & 0xFF) << 24) | ((buf[i+1] & 0xFF) << 16) | ((buf[i+2] & 0xFF) << 8) | (buf[i+3] & 0xFF);
  }

  private static boolean readIntEquals(byte[] buf, int i, int j) {
    return readInt(buf, i) == readInt(buf, j);
  }

  private static int commonBytes(byte[] b, int o1, int o2, int limit) {
    assert o1 < o2;
    int count = 0;
    while (o2 < limit && b[o1++] == b[o2++]) {
      ++count;
    }
    return count;
  }

  private static int commonBytesBackward(byte[] b, int o1, int o2, int l1, int l2) {
    int count = 0;
    while (o1 > l1 && o2 > l2 && b[--o1] == b[--o2]) {
      ++count;
    }
    return count;
  }

  /**
   * Decompress at least <code>decompressedLen</code> bytes into
   * <code>dest[dOff:]</code>. Please note that <code>dest</code> must be large
   * enough to be able to hold <b>all</b> decompressed data (meaning that you
   * need to know the total decompressed length).
   */
  public static int decompress(DataInput compressed, int decompressedLen, byte[] dest, int dOff) throws IOException {
    final int destEnd = dest.length;

    do {
      // literals
      final int token = compressed.readByte() & 0xFF;
      int literalLen = token >>> 4;

      if (literalLen != 0) {
        if (literalLen == 0x0F) {
          byte len;
          while ((len = compressed.readByte()) == (byte) 0xFF) {
            literalLen += 0xFF;
          }
          literalLen += len & 0xFF;
        }
        compressed.readBytes(dest, dOff, literalLen);
        dOff += literalLen;
      }

      if (dOff >= decompressedLen) {
        break;
      }

      // matchs
      final int matchDec = (compressed.readByte() & 0xFF) | ((compressed.readByte() & 0xFF) << 8);
      assert matchDec > 0;

      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        int len;
        while ((len = compressed.readByte()) == (byte) 0xFF) {
          matchLen += 0xFF;
        }
        matchLen += len & 0xFF;
      }
      matchLen += MIN_MATCH;

      // copying a multiple of 8 bytes can make decompression from 5% to 10% faster
      final int fastLen = ((matchLen - 1) & 0xFFFFFFF8) + 8;
      if (matchDec < matchLen || dOff + fastLen > destEnd) {
        // overlap -> naive incremental copy
        for (int ref = dOff - matchDec, end = dOff + matchLen; dOff < end; ++ref, ++dOff) {
          dest[dOff] = dest[ref];
        }
      } else {
        // no overlap -> arraycopy
        System.arraycopy(dest, dOff - matchDec, dest, dOff, fastLen);
        dOff += matchLen;
      }
    } while (dOff < decompressedLen);

    return dOff;
  }

  private static void encodeLen(int l, DataOutput out) throws IOException {
    while (l >= 0xFF) {
      out.writeByte((byte) 0xFF);
      l -= 0xFF;
    }
    out.writeByte((byte) l);
  }

  private static void encodeLiterals(byte[] bytes, int token, int anchor, int literalLen, DataOutput out) throws IOException {
    out.writeByte((byte) token);

    // encode literal length
    if (literalLen >= 0x0F) {
      encodeLen(literalLen - 0x0F, out);
    }

    // encode literals
    out.writeBytes(bytes, anchor, literalLen);
  }

  private static void encodeLastLiterals(byte[] bytes, int anchor, int literalLen, DataOutput out) throws IOException {
    final int token = Math.min(literalLen, 0x0F) << 4;
    encodeLiterals(bytes, token, anchor, literalLen, out);
  }

  private static void encodeSequence(byte[] bytes, int anchor, int matchRef, int matchOff, int matchLen, DataOutput out) throws IOException {
    final int literalLen = matchOff - anchor;
    assert matchLen >= 4;
    // encode token
    final int token = (Math.min(literalLen, 0x0F) << 4) | Math.min(matchLen - 4, 0x0F);
    encodeLiterals(bytes, token, anchor, literalLen, out);

    // encode match dec
    final int matchDec = matchOff - matchRef;
    assert matchDec > 0 && matchDec < 1 << 16;
    out.writeByte((byte) matchDec);
    out.writeByte((byte) (matchDec >>> 8));

    // encode match len
    if (matchLen >= MIN_MATCH + 0x0F) {
      encodeLen(matchLen - 0x0F - MIN_MATCH, out);
    }
  }

  /**
   * Compress <code>bytes[off:off+len]</code> into <code>out</code> using
   * at most 16KB of memory.
   */
  public static void compress(byte[] bytes, int off, int len, DataOutput out) throws IOException {

    final int base = off;
    final int end = off + len;

    int anchor = off++;

    if (len > LAST_LITERALS + MIN_MATCH) {

      final int limit = end - LAST_LITERALS;
      final int matchLimit = limit - MIN_MATCH;

      final int bitsPerOffset = PackedInts.bitsRequired(len - LAST_LITERALS);
      final int bitsPerOffsetLog = 32 - Integer.numberOfLeadingZeros(bitsPerOffset - 1);
      final int hashLog = MEMORY_USAGE + 3 - bitsPerOffsetLog;
      final PackedInts.Mutable hashTable = PackedInts.getMutable(1 << hashLog, bitsPerOffset, PackedInts.DEFAULT);

      main:
      while (off < limit) {
        // find a match
        int ref;
        while (true) {
          if (off >= matchLimit) {
            break main;
          }
          final int v = readInt(bytes, off);
          final int h = hash(v, hashLog);
          ref = base + (int) hashTable.get(h);
          assert PackedInts.bitsRequired(off - base) <= hashTable.getBitsPerValue();
          hashTable.set(h, off - base);
          if (off - ref < MAX_DISTANCE && readInt(bytes, ref) == v) {
            break;
          }
          ++off;
        }

        // compute match length
        final int matchLen = MIN_MATCH + commonBytes(bytes, ref + 4, off + 4, limit);

        encodeSequence(bytes, anchor, ref, off, matchLen, out);
        off += matchLen;
        anchor = off;
      }
    }

    // last literals
    final int literalLen = end - anchor;
    assert literalLen >= LAST_LITERALS || literalLen == len;
    encodeLastLiterals(bytes, anchor, end - anchor, out);
  }

  private static class Match {
    int start, ref, len;

    void fix(int correction) {
      start += correction;
      ref += correction;
      len -= correction;
    }

    int end() {
      return start + len;
    }
  }

  private static void copyTo(Match m1, Match m2) {
    m2.len = m1.len;
    m2.start = m1.start;
    m2.ref = m1.ref;
  }

  private static class HashTable {
    static final int MAX_ATTEMPTS = 256;
    static final int MASK = MAX_DISTANCE - 1;
    int nextToUpdate;
    private final int base;
    private final int[] hashTable;
    private final short[] chainTable;

    HashTable(int base) {
      this.base = base;
      nextToUpdate = base;
      hashTable = new int[HASH_TABLE_SIZE_HC];
      Arrays.fill(hashTable, -1);
      chainTable = new short[MAX_DISTANCE];
    }

    private int hashPointer(byte[] bytes, int off) {
      final int v = readInt(bytes, off);
      final int h = hashHC(v);
      return base + hashTable[h];
    }

    private int next(int off) {
      return base + off - (chainTable[off & MASK] & 0xFFFF);
    }

    private void addHash(byte[] bytes, int off) {
      final int v = readInt(bytes, off);
      final int h = hashHC(v);
      int delta = off - hashTable[h];
      if (delta >= MAX_DISTANCE) {
        delta = MAX_DISTANCE - 1;
      }
      chainTable[off & MASK] = (short) delta;
      hashTable[h] = off - base;
    }

    void insert(int off, byte[] bytes) {
      for (; nextToUpdate < off; ++nextToUpdate) {
        addHash(bytes, nextToUpdate);
      }
    }

    boolean insertAndFindBestMatch(byte[] buf, int off, int matchLimit, Match match) {
      match.start = off;
      match.len = 0;

      insert(off, buf);

      int ref = hashPointer(buf, off);
      for (int i = 0; i < MAX_ATTEMPTS; ++i) {
        if (ref < Math.max(base, off - MAX_DISTANCE + 1)) {
          break;
        }
        if (buf[ref + match.len] == buf[off + match.len] && readIntEquals(buf, ref, off)) {
          final int matchLen = MIN_MATCH + commonBytes(buf, ref + MIN_MATCH, off + MIN_MATCH, matchLimit);
          if (matchLen > match.len) {
            match.ref = ref;
            match.len = matchLen;
          }
        }
        ref = next(ref);
      }

      return match.len != 0;
    }

    boolean insertAndFindWiderMatch(byte[] buf, int off, int startLimit, int matchLimit, int minLen, Match match) {
      match.len = minLen;

      insert(off, buf);

      final int delta = off - startLimit;
      int ref = hashPointer(buf, off);
      for (int i = 0; i < MAX_ATTEMPTS; ++i) {
        if (ref < Math.max(base, off - MAX_DISTANCE + 1)) {
          break;
        }
        if (buf[ref - delta + match.len] == buf[startLimit + match.len]
            && readIntEquals(buf, ref, off)) {
          final int matchLenForward = MIN_MATCH + commonBytes(buf, ref + MIN_MATCH, off + MIN_MATCH, matchLimit);
          final int matchLenBackward = commonBytesBackward(buf, ref, off, base, startLimit);
          final int matchLen = matchLenBackward + matchLenForward;
          if (matchLen > match.len) {
            match.len = matchLen;
            match.ref = ref - matchLenBackward;
            match.start = off - matchLenBackward;
          }
        }
        ref = next(ref);
      }

      return match.len > minLen;
    }

  }

  /**
   * Compress <code>bytes[off:off+len]</code> into <code>out</code>. Compared to
   * {@link LZ4#compress(byte[], int, int, DataOutput)}, this method is slower,
   * uses more memory (~ 256KB), but should provide better compression ratios
   * (especially on large inputs) because it chooses the best match among up to
   * 256 candidates and then performs trade-offs to fix overlapping matches.
   */
  public static void compressHC(byte[] src, int srcOff, int srcLen, DataOutput out) throws IOException {

    final int srcEnd = srcOff + srcLen;
    final int matchLimit = srcEnd - LAST_LITERALS;

    int sOff = srcOff;
    int anchor = sOff++;

    final HashTable ht = new HashTable(srcOff);
    final Match match0 = new Match();
    final Match match1 = new Match();
    final Match match2 = new Match();
    final Match match3 = new Match();

    main:
    while (sOff < matchLimit) {
      if (!ht.insertAndFindBestMatch(src, sOff, matchLimit, match1)) {
        ++sOff;
        continue;
      }

      // saved, in case we would skip too much
      copyTo(match1, match0);

      search2:
      while (true) {
        assert match1.start >= anchor;
        if (match1.end() >= matchLimit
            || !ht.insertAndFindWiderMatch(src, match1.end() - 2, match1.start + 1, matchLimit, match1.len, match2)) {
          // no better match
          encodeSequence(src, anchor, match1.ref, match1.start, match1.len, out);
          anchor = sOff = match1.end();
          continue main;
        }

        if (match0.start < match1.start) {
          if (match2.start < match1.start + match0.len) { // empirical
            copyTo(match0, match1);
          }
        }
        assert match2.start > match1.start;

        if (match2.start - match1.start < 3) { // First Match too small : removed
          copyTo(match2, match1);
          continue search2;
        }

        search3:
        while (true) {
          if (match2.start - match1.start < OPTIMAL_ML) {
            int newMatchLen = match1.len;
            if (newMatchLen > OPTIMAL_ML) {
              newMatchLen = OPTIMAL_ML;
            }
            if (match1.start + newMatchLen > match2.end() - MIN_MATCH) {
              newMatchLen = match2.start - match1.start + match2.len - MIN_MATCH;
            }
            final int correction = newMatchLen - (match2.start - match1.start);
            if (correction > 0) {
              match2.fix(correction);
            }
          }

          if (match2.start + match2.len >= matchLimit
              || !ht.insertAndFindWiderMatch(src, match2.end() - 3, match2.start, matchLimit, match2.len, match3)) {
            // no better match -> 2 sequences to encode
            if (match2.start < match1.end()) {
              if (match2.start - match1.start < OPTIMAL_ML) {
                if (match1.len > OPTIMAL_ML) {
                  match1.len = OPTIMAL_ML;
                }
                if (match1.end() > match2.end() - MIN_MATCH) {
                  match1.len = match2.end() - match1.start - MIN_MATCH;
                }
                final int correction = match1.len - (match2.start - match1.start);
                if (correction > 0) {
                  match2.fix(correction);
                }
              } else {
                match1.len = match2.start - match1.start;
              }
            }
            // encode seq 1
            encodeSequence(src, anchor, match1.ref, match1.start, match1.len, out);
            anchor = sOff = match1.end();
            // encode seq 2
            encodeSequence(src, anchor, match2.ref, match2.start, match2.len, out);
            anchor = sOff = match2.end();
            continue main;
          }

          if (match3.start < match1.end() + 3) { // Not enough space for match 2 : remove it
            if (match3.start >= match1.end()) { // // can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1
              if (match2.start < match1.end()) {
                final int correction = match1.end() - match2.start;
                match2.fix(correction);
                if (match2.len < MIN_MATCH) {
                  copyTo(match3, match2);
                }
              }

              encodeSequence(src, anchor, match1.ref, match1.start, match1.len, out);
              anchor = sOff = match1.end();

              copyTo(match3, match1);
              copyTo(match2, match0);

              continue search2;
            }

            copyTo(match3, match2);
            continue search3;
          }

          // OK, now we have 3 ascending matches; let's write at least the first one
          if (match2.start < match1.end()) {
            if (match2.start - match1.start < 0x0F) {
              if (match1.len > OPTIMAL_ML) {
                match1.len = OPTIMAL_ML;
              }
              if (match1.end() > match2.end() - MIN_MATCH) {
                match1.len = match2.end() - match1.start - MIN_MATCH;
              }
              final int correction = match1.end() - match2.start;
              match2.fix(correction);
            } else {
              match1.len = match2.start - match1.start;
            }
          }

          encodeSequence(src, anchor, match1.ref, match1.start, match1.len, out);
          anchor = sOff = match1.end();

          copyTo(match2, match1);
          copyTo(match3, match2);

          continue search3;
        }

      }

    }

    encodeLastLiterals(src, anchor, srcEnd - anchor, out);
  }

  /** Copy bytes from <code>in</code> to <code>out</code> where
   *  <code>in</code> is a LZ4-encoded stream. This method copies enough bytes
   *  so that <code>out</code> can be used later on to restore the first
   *  <code>length</code> bytes of the stream. This method always reads at
   *  least one byte from <code>in</code> so make sure not to call this method
   *  if <code>in</code> reached the end of the stream, even if
   *  <code>length=0</code>. */
  public static int copyCompressedData(DataInput in, int length, DataOutput out) throws IOException {
    int n = 0;
    do {
      // literals
      final byte token = in.readByte();
      out.writeByte(token);
      int literalLen = (token & 0xFF) >>> 4;
      if (literalLen == 0x0F) {
        byte len;
        while ((len = in.readByte()) == (byte) 0xFF) {
          literalLen += 0xFF;
          out.writeByte(len);
        }
        literalLen += len & 0xFF;
        out.writeByte(len);
      }
      out.copyBytes(in, literalLen);
      n += literalLen;
      if (n >= length) {
        break;
      }

      // matchs
      out.copyBytes(in, 2); // match dec
      int matchLen = token & 0x0F;
      if (matchLen == 0x0F) {
        byte len;
        while ((len = in.readByte()) == (byte) 0xFF) {
          matchLen += 0xFF;
          out.writeByte(len);
        }
        matchLen += len & 0xFF;
        out.writeByte(len);
      }
      matchLen += MIN_MATCH;
      n += matchLen;
    } while (n < length);
    return n;
  }

}