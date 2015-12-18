package whelk

import java.util.zip.CRC32

class IdGenerator {

    static final char[] ALPHANUM = "0123456789abcdefghijklmnopqrstuvwxyz".chars
    static final char[] VOWELS = "aoueiy".chars
    static final char[] CONSONANTS = ALPHANUM.findAll { !VOWELS.contains(it) } as char[]

    static final int PAD_LENGTH = 7

    static char[] alphabet = CONSONANTS

    static String generate() {
        return generate(System.currentTimeMillis(), null)
    }

    static String generate(long timestamp, String data, int paddLength = PAD_LENGTH) {
        StringBuilder pad = new StringBuilder()
        if (data) {
            String check = checksum(data)
            if (check.length() > paddLength) {
                check = check.substring(0, paddLength)
            }
            pad.append(check)
            while (pad.length() < paddLength) {
                pad.insert(0, '0')
            }
        } else {
            while (pad.length() < paddLength) {
                pad.append(CONSONANTS[new Random().nextInt(CONSONANTS.length)])
            }
        }
        return baseEncode(timestamp, true) + pad.toString()
    }

    static String checksum(String data) {
        CRC32 crc32 = new CRC32()
        crc32.update(data.getBytes("UTF-8"))
        return baseEncode(crc32.value, false)
    }

    static String baseEncode(long n, boolean lastDigitBasedCaesarCipher=false) {
        int base = alphabet.length
        int[] positions = basePositions(n, base)
        if (lastDigitBasedCaesarCipher) {
            rotate(positions, base)
        }
        return baseEncode((int[]) positions)
    }

    static int[] basePositions(long n, int base) {
        int maxExp = Math.floor(Math.log(n) / Math.log(base))
        int[] positions = new int[maxExp + 1]
        for (int i=maxExp; i > -1; i--) {
            positions[maxExp-i] = (int) (((n / (base ** i)) as long) % base)
        }
        return positions
    }

    static String baseEncode(int[] positions) {
        def chars = positions.collect { alphabet[it] }
        return chars.join("")
    }

    static void rotate(int[] positions, int ceil) {
        int rotation = positions[-1]
        for (int i=0; i < positions.length - 1; i++) {
            positions[i] = rotatePosition(positions[i], rotation, ceil)
        }
    }

    static int rotatePosition(int i, int rotation, int ceil) {
        int j = i + rotation
        return (j >= ceil)? j - ceil : j
    }

}
