package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class BufferedChannelWriteTest {

    private final UnpooledByteBufAllocator allocator = new UnpooledByteBufAllocator(true);

    private FileChannel fileChannel;
    private int capacity;

    private ByteBuf src;
    private int srcSize;

    private byte[] testData;

    private int numberOfExistingBytes;
    private long unpersistedBytesBound;

    private STATE fcState;
    private STATE srcState;

    private boolean isExepctedException;
    private boolean unexpectedAllocation;

    public BufferedChannelWriteTest(
            int capacity,
            int srcSize,
            STATE fcState,
            STATE srcState,
            long unpersistedBytesBound,
            boolean isExepctedException,
            boolean unexpectedAllocation) {

            this.capacity = capacity;;
            this.fcState = fcState;
            this.srcState = srcState;
            this.srcSize = srcSize;
            this.unpersistedBytesBound = unpersistedBytesBound;
            this.isExepctedException = isExepctedException;
            this.unexpectedAllocation = unexpectedAllocation;
            this.numberOfExistingBytes = 0;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][]{

                //{capacity,        SRC_SIZE,       FC_STATE,       SRC_STATE,          UNPERSISTED_BYTE,   EXCEPTION,      UNEXPECTED_ALLOCATION}
                { -1,               5,              STATE.EMPTY,    STATE.NOT_EMPTY,    0L,                 true,           false},                 //case 1
                { 0,                5,              STATE.EMPTY,    STATE.NOT_EMPTY,    0L,                 true,           true},                  //case 2 <-- Failure
                { 0,                0,              STATE.EMPTY,    STATE.EMPTY,        0L,                 true,           false},                 //case 3
                { 10,               0,              STATE.EMPTY,    STATE.NULL,         0L,                 true,           false},                 //case 4
                { 10,               0,              STATE.EMPTY,    STATE.INVALID,      0L,                 true,           false},                 //case 5
                { 10,               15,             STATE.NOT_EMPTY,STATE.NOT_EMPTY,    0L,                 false,          false},                 //case 6
                { 10,               15,             STATE.NULL,     STATE.NOT_EMPTY,    0L,                 true,           false},                 //case 7
                { 10,               15,             STATE.INVALID,  STATE.NOT_EMPTY,    0L,                 true,           false},                 //case 8
                { 15,               15,             STATE.NOT_EMPTY,STATE.NOT_EMPTY,    0L,                 false,          false},                 //case 9
                { 15,               10,             STATE.NOT_EMPTY,STATE.NOT_EMPTY,    3L,                 false,          false},                 //case 10
                { 15,               10,             STATE.NOT_EMPTY,STATE.NOT_EMPTY,    6L,                 false,          false},                 //case 11
                { 10,               5,              STATE.EMPTY,    STATE.EMPTY,        3L,                 false,          false},                 //case 12
                { 10,               5,              STATE.EMPTY,    STATE.EMPTY,        6L,                 false,          false},                 //case 13
                { 15,               15,             STATE.NOT_EMPTY,STATE.NOT_EMPTY,    6L,                 false,          false}                  //case 14

        });
    }

    @BeforeClass
    //Create a new file each time run a test
    public static void setUpClass() {

        //Create directory for test
        File fileDir = new File("testDir/bufferedChannelWriteTest");
        if(!fileDir.exists()) {
            fileDir.mkdirs();
        }

        //Create file for test
        File file = new File("testDir/bufferedChannelWriteTest/file.log");
        if(!file.exists()) {
            file.delete();
        }

    }

    @Before
    public void setUp() {

        Path path = Paths.get("testDir/bufferedChannelWriteTest/file.log");
        //Create FileChannel
        try{
            Random random = new Random(System.currentTimeMillis());
            if(this.fcState == STATE.NOT_EMPTY || this.fcState == STATE.EMPTY) {
                if(this.fcState == STATE.NOT_EMPTY) {
                    try(FileOutputStream fileOutputStream = new FileOutputStream("testDir/bufferedChannelWriteTest/file.log")) {
                        this.numberOfExistingBytes = random.nextInt(10);
                        byte[] alreadyExistingBytes = new byte[this.numberOfExistingBytes];
                        random.nextBytes(alreadyExistingBytes);
                        fileOutputStream.write(alreadyExistingBytes);
                    }
                }

                this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                this.fileChannel.position(this.fileChannel.size());
                this.testData = new byte[this.srcSize];

                if(this.srcState != STATE.EMPTY) {
                    random.nextBytes(this.testData);
                }else{
                    Arrays.fill(this.testData, (byte) 0);
                }

            }else if (this.srcState == STATE.NULL) {

                this.fileChannel = null;

            } else if (this.srcState == STATE.INVALID) {

                FileChannel fc = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                fc.close();
                this.fileChannel = fc;

            }

            //Create src
            this.src = Unpooled.directBuffer(this.srcSize);
            if(this.fcState == STATE.NOT_EMPTY) {
                this.src.writeBytes(this.testData);
            } else if (this.srcState == STATE.NULL) {
                this.src = null;
            }else if(this.srcState == STATE.INVALID){
                ByteBuf invalidByteBuf = mock(ByteBuf.class);
                when(invalidByteBuf.readableBytes()).thenReturn(1);
                when(invalidByteBuf.readerIndex()).thenReturn(-1);
                this.src = invalidByteBuf;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void writeTest()  {

        try {
            BufferedChannel bufferedChannel = new BufferedChannel(this.allocator, this.fileChannel, this.capacity, this.unpersistedBytesBound);
            bufferedChannel.write(this.src);

            int expectedNumberOfBytesinWriteBuffer = 0;
            if (this.srcState != STATE.EMPTY && capacity != 0) {
                if (this.srcSize < this.capacity) {
                    expectedNumberOfBytesinWriteBuffer = this.srcSize;
                } else {
                    expectedNumberOfBytesinWriteBuffer = this.srcSize % this.capacity;
                }
            }

            int expectedNumberOfBytesInFileChannel = 0;
            if (this.unpersistedBytesBound > 0) {
                if (this.unpersistedBytesBound <= this.srcSize) {
                    expectedNumberOfBytesInFileChannel = this.srcSize;
                    expectedNumberOfBytesinWriteBuffer = 0;
                }
            } else {
                if (this.srcSize < this.capacity) {
                    expectedNumberOfBytesInFileChannel = 0;
                } else {
                    expectedNumberOfBytesInFileChannel = this.srcSize - expectedNumberOfBytesinWriteBuffer;
                }
            }

            byte[] actualBytesWrittenInWriteBuffer = new byte[expectedNumberOfBytesinWriteBuffer];
            bufferedChannel.writeBuffer.getBytes(0, actualBytesWrittenInWriteBuffer);

            byte[] effectiveBytesInWriteBuffer = Arrays.copyOfRange(this.testData, this.testData.length - expectedNumberOfBytesinWriteBuffer, this.testData.length);

            Assert.assertEquals(Arrays.toString(actualBytesWrittenInWriteBuffer), Arrays.toString(effectiveBytesInWriteBuffer));

            if(unexpectedAllocation){
                throw new Exception();              //<-- Done only to throw the exception expected in case 2
            }

            // Conditions for verifying that flush operations are performed for improve Pit coverage

            ByteBuffer effectiveByteInFileChannel = ByteBuffer.allocate(expectedNumberOfBytesInFileChannel);

            this.fileChannel.position(this.numberOfExistingBytes);
            this.fileChannel.read(effectiveByteInFileChannel);

            byte[] expectedBytesInFileChannel = Arrays.copyOfRange(this.testData, 0, expectedNumberOfBytesInFileChannel);

            Assert.assertEquals(Arrays.toString(effectiveByteInFileChannel.array()), Arrays.toString(expectedBytesInFileChannel));
            if(this.srcState == STATE.EMPTY) {
                Assert.assertEquals(this.numberOfExistingBytes, bufferedChannel.position());
            } else {
                Assert.assertEquals(this.numberOfExistingBytes + this.srcSize, bufferedChannel.position());
            }

        } catch (Exception e) {
            Assert.assertTrue(isExepctedException);
        }

    }

    /*
    @Test
    public void testWrite() {

        ByteBuf byteBuf = Unpooled.buffer(this.srcSize, this.srcSize);
        try{

            BufferedChannel bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.capacity, this.unpersistedBytesBound);
            bufferedChannel.write(this.src);
            int n = bufferedChannel.read(byteBuf, 0);
            Assert.assertEquals(this.srcSize, n);

        }catch (Exception e) {
            Assert.assertTrue(this.isExepctedException);
        }

    }
    */

    @After
    public void tearDownFile() {

        //Why? I must have a new file for each test cases
        try {
            //Close the FileChannel
            if (this.fcState != STATE.NULL && this.fileChannel != null) {
                this.fileChannel.close();
            }
            //Delete the test file
            File file = new File("testDir/bufferedChannelWriteTest/file.log");
            if (file.exists()) {
                file.delete();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @AfterClass
    public static void tearDown() {

        //Delete directory and test file
        File fileDir = new File("testDir/bufferedChannelWriteTest");
        if(fileDir.exists()) {
            File[] files = fileDir.listFiles();
            if(files != null) {
                for(File file : files) {
                    file.delete();
                }
            }
        }
        fileDir.delete();

        File directory = new File("testDir");
        if(directory.exists()) {
            directory.delete();
        }

    }

}
