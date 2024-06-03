package org.apache.bookkeeper.util;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.util.*;
import java.util.stream.LongStream;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesConstructorsTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;
    private PrimitiveIterator.OfLong entriesOfLongIterator;
    private long[] entriesOfLedger;
    private byte[] serializeStateOfEntriesOfLedger;
    private ByteBuf byteBuf;
    private boolean isExpectedException;
    private boolean isExpectedFailulre;
    private CONSTRUCTOR typeOfConstructor;

    public AvailabilityOfEntriesConstructorsTest(
        long[] entriesOfLedger,
        CONSTRUCTOR typeOfConstructor,
        boolean isExpectedException,
        boolean isExpectedFailulre) {

        this.entriesOfLedger = entriesOfLedger;
        this.typeOfConstructor = typeOfConstructor;
        this.isExpectedException = isExpectedException;
        this.isExpectedFailulre = isExpectedFailulre;
        createEntriesOfLedger();

    }

    private void createEntriesOfLedger() {

        switch (typeOfConstructor) {
            case ARRAY_LONG:
                break;
            case ITERATOR:
                if (this.entriesOfLedger != null) {
                    LongStream longStream = Arrays.stream(entriesOfLedger);
                    this.entriesOfLongIterator = longStream.iterator();
                }else{
                    this.entriesOfLongIterator = null;
                }
                break;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        long[] valid = new long[]{0L, 1L, 4L, 5L, 8L};
        long[] invalid = new long[]{0L, -1L, 4L, -5L, -8L};
        long[] empty = new long[5];

        return Arrays.asList(new Object[][] {
                //{entriesOfLedger,     TYPE_OF_CONSTRUCTORS,           EXCEPTION,      FAILURE}
                {valid,                 CONSTRUCTOR.ARRAY_LONG,         false,          false},         //case 1
                {invalid,               CONSTRUCTOR.ARRAY_LONG,         true,           true},          //case 2 <-- Failure
                {empty,                 CONSTRUCTOR.ARRAY_LONG,         false,          false},         //case 3
                {null,                  CONSTRUCTOR.ARRAY_LONG,         true,           false},         //case 4
                {valid,                 CONSTRUCTOR.ITERATOR,           false,          false},         //case 5
                {invalid,               CONSTRUCTOR.ITERATOR,           true,           true},          //case 6 <-- Failure
                {empty,                 CONSTRUCTOR.ITERATOR,           false,          false},         //case 7
                {null,                  CONSTRUCTOR.ITERATOR,           true,           false},         //case 8
                {valid,                 CONSTRUCTOR.ARRAY_BYTE,         false,          false},         //case 9
                {invalid,               CONSTRUCTOR.ARRAY_BYTE,         true,           true},          //case 10 <-- Failure
                {empty,                 CONSTRUCTOR.ARRAY_BYTE,         false,          false},         //case 11
                {null,                  CONSTRUCTOR.ARRAY_BYTE,         true,           false},         //case 12
                {valid,                 CONSTRUCTOR.BYTE_BUF,           false,          false},         //case 13
                {invalid,               CONSTRUCTOR.BYTE_BUF,           true,           true},          //case 14 <-- Failure
                {empty,                 CONSTRUCTOR.BYTE_BUF,           false,          false},         //case 15
                {null,                  CONSTRUCTOR.BYTE_BUF,           true,           false}          //case 16
        });
    }

    private int count(long[] content) {

        List<Long> list = new ArrayList<>();
        PrimitiveIterator.OfLong iterator = Arrays.stream(content).iterator();

        while (iterator.hasNext()) {
            Long l = iterator.next();
            if(!list.contains(l) && l >= 0) {
                list.add(l);
            }
        }
        return list.size();

    }

    @Test
    public void constructorTest() {

        try{
            switch (this.typeOfConstructor) {
                case ARRAY_LONG:
                    this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.entriesOfLedger);
                    break;
                case ITERATOR:
                    this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.entriesOfLongIterator);
                    break;
                case ARRAY_BYTE:
                    this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.entriesOfLedger);
                    this.serializeStateOfEntriesOfLedger = this.availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger();
                    this.availabilityOfEntriesOfLedger = null;
                    this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.serializeStateOfEntriesOfLedger);
                    break;
                case BYTE_BUF:
                    this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.entriesOfLedger);
                    this.byteBuf = Unpooled.buffer();
                    this.byteBuf.writeBytes(this.availabilityOfEntriesOfLedger.serializeStateOfEntriesOfLedger());
                    this.availabilityOfEntriesOfLedger = null;
                    this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.byteBuf);
                    break;
            }

            /*
            for(int i = 0; i < this.entriesOfLedger.length; i++){
                for(long l : this.entriesOfLedger){
                    System.out.println("entry:" + l);
                    if(this.availabilityOfEntriesOfLedger.isEntryAvailable(l)){
                        System.out.println("entry inside:" + l);
                    }
                }
            }
            */

            if (!this.isExpectedFailulre) {
                Assert.assertEquals(count(this.entriesOfLedger), this.availabilityOfEntriesOfLedger.getTotalNumOfAvailableEntries());
            }

            for (long entry : this.entriesOfLedger) {
                if(entry >= 0 && !this.isExpectedFailulre) {
                    Assert.assertTrue(this.availabilityOfEntriesOfLedger.isEntryAvailable(entry));
                }else {
                    Assert.assertTrue(this.isExpectedFailulre);
                }

            }
        }catch (Exception e){
            Assert.assertTrue(isExpectedException);
        }

    }

}
