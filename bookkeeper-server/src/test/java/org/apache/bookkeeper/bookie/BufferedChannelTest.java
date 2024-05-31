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
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelTest {

    private enum FC_STATE {
        NOT_EMPTY,
        NULL,
        EMPTY,
        INVALID
    }

    private enum DEST_STATE {
        VALID,
        NULL,
        INVALID
    }

    private BufferedChannel bufferedChannel;
    private FileChannel fileChannel;
    private ByteBuf readBuffer;
    private int startingPosition;
    private int capacity;
    private int length;
    private int fileSize;
    private byte[] testData;
    private FC_STATE fc_state;
    private DEST_STATE dest_state;
    private Random random = new Random(System.currentTimeMillis());
    private boolean isExpectedException;
    private boolean unexpectedNumberOfBytesRead;
    private boolean writeBefore;

    public BufferedChannelTest(
            int capacity,
            FC_STATE fcState,
            DEST_STATE destState,
            int startingPosition,
            int length,
            int fileSize,
            boolean isExpectedException,
            boolean unexpectedNumberOfBytesRead,
            boolean writeBefore) {

        this.capacity = capacity;
        this.fc_state = fcState;
        this.dest_state = destState;
        this.startingPosition = startingPosition;
        this.length = length;
        this.fileSize = fileSize;
        this.isExpectedException = isExpectedException;
        this.unexpectedNumberOfBytesRead = unexpectedNumberOfBytesRead;
        this.writeBefore = writeBefore;

    }

    //7, 8, 10, 11, 12, 13, 14

    @Parameterized.Parameters
    public static Collection<?> getParameters() {
        return Arrays.asList(new Object[][] {
                //      {capacity,  FC_STATE,               DEST_STATE,     pos,    length,     file_size,          exception,      unexpecetedRead,    writeBefore}
                { -1,        FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      0,      14,         15,                 true,           false,              false},             //case 1
                // Founded failure, read more bytes than expected
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      0,      14,         15,                 false,          true,               false},             //case 2
                { 0,         FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      0,      14,         15,                 true,           false,              false},             //case 3
                { 0,         FC_STATE.NOT_EMPTY,     DEST_STATE.INVALID,    0,      14,         15,                 true,           false,              false},             //case 4
                { 15,        FC_STATE.NULL,          DEST_STATE.VALID,      0,      14,         15,                 true,           false,              false},             //case 5
                { 15,        FC_STATE.INVALID,       DEST_STATE.VALID,      0,      15,         15,                 true,           false,              false},             //case 6
                { 15,        FC_STATE.EMPTY,         DEST_STATE.VALID,      0,      0,          0,                  false,          false,              false},             //case 7
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.NULL,       0,      1,          15,                 true,           false,              false},             //case 8
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.INVALID,    0,      14,         15,                 true,           false,              false},             //case 9
                { 15,        FC_STATE.EMPTY,         DEST_STATE.INVALID,    0,      14,         15,                 true,           false,              false},             //case 10
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      15,     14,         15,                 true,           false,              false},             //case 11
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      16,     14,         15,                 true,           false,              false},             //case 12
                { 15,        FC_STATE.EMPTY,         DEST_STATE.INVALID,    0,      14,         15,                 true,           false,              false},             //case 13
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      0,      16,         15,                 true,           false,              false},             //case 14
                // Founded failure, read more bytes than expected
                { 15,        FC_STATE.EMPTY,         DEST_STATE.VALID,      0,      1,          2,                  false,          true,               true},              //case 15
                { 15,        FC_STATE.EMPTY,         DEST_STATE.VALID,      0,      0,          0,                  false,          false,              true},              //case 16
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.NULL,       0,      1,          15,                 true,           false,              true},              //case 17
                { 15,        FC_STATE.EMPTY,         DEST_STATE.INVALID,    0,      1,          15,                 true,           false,              true},              //case 18
                { 15,        FC_STATE.EMPTY,         DEST_STATE.VALID,      15,     14,         15,                 true,           true,               true},              //case 19
                { 15,        FC_STATE.NOT_EMPTY,     DEST_STATE.VALID,      16,     14,         15,                 true,           false,              true},              //case 20
                { 15,        FC_STATE.EMPTY,         DEST_STATE.INVALID,    0,      15,         15,                 true,           false,              true},              //case 21
                // Founded failure, read more bytes than expected
                { 15,        FC_STATE.EMPTY,         DEST_STATE.VALID,      0,      15,         15,                 false,          true,               true},              //case 22
                { 15,        FC_STATE.EMPTY,         DEST_STATE.VALID,      0,      1,          0,                  true,           false,              true}               //case 23

        });
    }

    @BeforeClass
    //Create a new file each time run a test
    public static void setUpClass() {

        //Create directory for test
        File fileDir = new File("testDir/bufferedChannelTest");
        if(!fileDir.exists()) {
            fileDir.mkdirs();
        }

        //Create file for test
        File file = new File("testDir/bufferedChannelTest/file.log");
        if(!file.exists()) {
            file.delete();
        }

    }

    @Before
    public void setUp() {

        //Create FileChannel
        try{
            if(this.fc_state == FC_STATE.NOT_EMPTY || this.fc_state == FC_STATE.EMPTY) {
                this.testData = new byte[this.fileSize];
                random.nextBytes(testData);
                if(this.fc_state == FC_STATE.NOT_EMPTY) {
                    try (FileOutputStream fileOutputStream = new FileOutputStream("testDir/bufferedChannelTest/file.log")){
                        fileOutputStream.write(testData);
                    }
                }

                this.fileChannel = FileChannel.open(Paths.get("testDir/bufferedChannelTest/file.log"), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
                this.fileChannel.position(this.fileChannel.size());

                if (this.fc_state == null) {
                    this.fileChannel = null;
                } else if(this.fc_state == FC_STATE.INVALID) {
                    this.fileChannel.close();
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Create ByteBuf
        switch(this.dest_state) {
            case VALID:
                this.readBuffer = Unpooled.buffer();
                break;
            case NULL:
                this.readBuffer = null;
                break;
            case INVALID:
                this.readBuffer = mock(ByteBuf.class);
                when(this.readBuffer.writableBytes()).thenReturn(1);
                when(this.readBuffer.writeBytes(any(ByteBuf.class), any(int.class), any(int.class))).thenThrow(new IndexOutOfBoundsException("Invalid ByteBuf"));
        }

    }

    @Test
    public void read(){

        try{
            this.bufferedChannel = new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, this.fileChannel, this.capacity);

            if(this.writeBefore) {
                ByteBuf byteBuf = Unpooled.buffer();
                byteBuf.writeBytes(testData);
                this.bufferedChannel.write(byteBuf);
            }

            int bytesRead = this.bufferedChannel.read(this.readBuffer, this.startingPosition, this.length);
            if(this.unexpectedNumberOfBytesRead){
                Assert.assertEquals(this.fileSize, bytesRead);
            }else {
                Assert.assertEquals(this.length, bytesRead);
            }

        }catch (Exception e) {
            Assert.assertTrue(this.isExpectedException);
        }

    }

    @After
    public void tearDownFile() {

        this.bufferedChannel = null;
        //Why? I must have a new file for each test cases
        try {
            //Close the FileChannel
            if (this.fc_state != FC_STATE.NULL && this.fileChannel != null) {
                this.fileChannel.close();
            }
            //Delete the test file
            File file = new File("testDir/bufferedChannelTest/file.log");
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
        File fileDir = new File("testDir/bufferedChannelTest");
        if(fileDir.exists()) {
            File[] files = fileDir.listFiles();
            if(files != null) {
                for(File file : files) {
                    file.delete();
                }
            }
        }
        File directory = new File("testDir");
        if(directory.exists()) {
            directory.delete();
        }

    }

}