import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Pigzj {

    /**
     * GZIP header magic number. From MessAdmin
     */
    public final static int GZIP_MAGIC = 0x8b1f;

    private final static byte[] default_header = {
            (byte) GZIP_MAGIC, // Magic number (short)
            (byte) (GZIP_MAGIC >> 8), // Magic number (short)
            Deflater.BEST_SPEED, // Compression method (CM)
            0, // Flags (FLG)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Modification time MTIME (int)
            0, // Extra flags (XFLG)
            (byte) 0xff // Operating system (OS): unknown
    };

    static byte[] dictionary = new byte['\u8000'];

    private static void writeGzip(int availableProcessors) throws IOException, InterruptedException {
        ArrayList<byte[]> bufferList = new ArrayList<>();

        int byteSize = 128 * 1024; // TODO
        byte[] buffer = new byte[byteSize];
        CRC32 crc = new CRC32();
        long uncompressedLength = 0;
        int bytesRead;

        while ((bytesRead = System.in.read(buffer)) != -1) {
            crc.update(buffer, 0, bytesRead);
            uncompressedLength += bytesRead;

            byte[] copy = new byte[bytesRead];
            System.arraycopy(buffer, 0, copy, 0, bytesRead);
            bufferList.add(copy);
        }

        ExecutorService executor = Executors.newFixedThreadPool(availableProcessors);
        List<Future<ByteArrayOutputStream>> futures = new ArrayList<>();

        for (int i = 0; i < bufferList.size(); i++) {
            byte[] bytes = bufferList.get(i);
            boolean isLastBlock = (i == bufferList.size() - 1);
            boolean isFirstBlock = (i == 0);
            if (!isFirstBlock) {
                byte[] previousBlock = bufferList.get(i - 1);
                int previousBlockSize = 32 * 1024;
                dictionary = new byte[previousBlockSize];
                System.arraycopy(previousBlock, previousBlock.length - previousBlockSize,
                        dictionary, 0,
                        previousBlockSize);
            }

            Future<ByteArrayOutputStream> future = executor.submit(() -> {
                Deflater deflater = new Deflater(Deflater.BEST_SPEED, true);
                ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
                byte[] compressBuffer = new byte[byteSize];

                deflater.setDictionary(dictionary);
                deflater.setInput(bytes);

                if (isLastBlock) {
                    deflater.finish();
                }

                int deflateMode = isLastBlock ? Deflater.FULL_FLUSH : Deflater.SYNC_FLUSH;
                while (!deflater.finished()) {
                    int compressedBytes = deflater.deflate(compressBuffer, 0, compressBuffer.length, deflateMode);
                    compressedOut.write(compressBuffer, 0, compressedBytes);
                    if (!isLastBlock) {
                        break;
                    }
                }
                if (isLastBlock) {
                    deflater.end();
                }
                return compressedOut;
            });
            futures.add(future);
        }

        ByteArrayOutputStream compressedData = new ByteArrayOutputStream();
        for (Future<ByteArrayOutputStream> future : futures) {
            try {
                compressedData.write(future.get().toByteArray());
            } catch (ExecutionException e) {
                System.err.println(e.getCause());
                e.printStackTrace();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println(e.getMessage());
            } catch (IOException e) {
                System.err.println(e.getMessage());
            }
        }

        executor.shutdown();

        System.out.write(default_header);
        System.out.write(compressedData.toByteArray());
        System.out.write(longToLE(crc.getValue()));
        System.out.write(longToLE(uncompressedLength % 0x100000000L));
        System.out.flush();
    }

    private static byte[] longToLE(long value) {
        return new byte[] {
                (byte) value,
                (byte) (value >> 8),
                (byte) (value >> 16),
                (byte) (value >> 24)
        };
    }

    public static void main(String[] args) {
        try {
            Runtime runtime = Runtime.getRuntime();
            int availableProcessors = runtime.availableProcessors();

            if (args.length >= 1 && args[1] != null && !args[1].isEmpty()) {
                availableProcessors = Integer.valueOf(args[1]);
            } else if (args.length >= 1) {
                throw new IllegalArgumentException("Third argument is required");
            }

            try {
                writeGzip(availableProcessors);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
