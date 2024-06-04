package org.apache.bookkeeper.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesOfLedgerGetUnavailableEntriesTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger((new long[]{}));
    private long startEntryId;
    private long lastEntryId;
    private BitSet bitSet;
    private BITSET_STATE bitsetState;
    private LIST_STATE listState;
    private boolean isExpectedException;
    private boolean sequenceEntries;
    private boolean nullable;

    public AvailabilityOfEntriesOfLedgerGetUnavailableEntriesTest(

            long startEntryId,
            long lastEntryId,
            BITSET_STATE bitsetState,
            LIST_STATE listState,
            boolean isExpectedException,
            boolean sequenceEntries,
            boolean nullable) {

            this.startEntryId = startEntryId;
            this.lastEntryId = lastEntryId;
            this.bitsetState = bitsetState;
            this.listState = listState;
            this.isExpectedException = isExpectedException;
            this.sequenceEntries = sequenceEntries;
            this.nullable = nullable;

    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        return Arrays.asList(new Object[][] {
                //{startEntryID,        lastEntryId,    BITSET_STATE,           LIST_STATE,             EXCEPTION,      SEQUENCE,   NULLABLE}
                {-1L,                   -2L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       true,           false,      false},         //case 1
                {-1L,                   -1L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       true,           false,      false},         //case 2
                { 0L,                   0L,             BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,      false},         //case 3
                { 0L,                   1L,             BITSET_STATE.INVALID,   LIST_STATE.NOT_EMPTY,   false,          false,      false},         //case 4
                { 0L,                   0L,             BITSET_STATE.INVALID,   LIST_STATE.EMPTY,       false,          false,      false},         //case 5
                { 0L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,      false},         //case 6
                {15L,                   15L,            BITSET_STATE.INVALID,   LIST_STATE.EMPTY,       false,          false,      false},         //case 7
                { 0L,                   15L,            BITSET_STATE.NULL,      LIST_STATE.EMPTY,       true,           false,      false},         //case 8
                {-1L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.NOT_EMPTY,   false,          false,      false},         //case 9
                {15L,                   14L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,      false},         //case 10
                { 0L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.NOT_EMPTY,   false,          true,       false},         //case 11 <-- SequencePeriod
                { 0L,                   15L,            BITSET_STATE.INVALID,   LIST_STATE.NOT_EMPTY,   false,          true,       false},         //case 12 <-- SequencePeriod
                { 0L,                   15L,            BITSET_STATE.EMPTY,     LIST_STATE.EMPTY,       false,          false,      true}           //case 12 <-- NoSequenceGroup

        });
    }

    @Before
    public void setUp() {

        long[] entries = {2L, 4L, 5L, 7L, 8L};
        long[] invalidEntries = {1L, 3L, 6L, 9L, 10L, 11L, 12L};
        long[] sequenceEntry = {3L, 5L, 7L, 9L};

        BitSet validBitset = new BitSet(10);
        BitSet invalidBitset = new BitSet(10);
        BitSet emptyBitset = new BitSet(10);
        BitSet notEmptyBitset = new BitSet(10);

        for(long entry : entries) {
            validBitset.set((int) entry);
        }

        for(long entry : invalidEntries) {
            invalidBitset.set((int) entry);
        }

        for(int i = 0; i < notEmptyBitset.length(); i++){
            notEmptyBitset.set(i);
        }

        switch (this.bitsetState) {
            case VALID:
                this.bitSet = validBitset;
                break;
            case INVALID:
                this.bitSet = invalidBitset;
                break;
            case EMPTY:
                this.bitSet = emptyBitset;
                break;
            case NULL:
                this.bitSet = null;
                break;
        }

        if(this.sequenceEntries) {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(sequenceEntry);
        } else if (this.nullable) {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger((new long[]{}));
        } else {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(entries);
        }
    }

    @Test
    public void getUnavailableEntriesTest() {

        List<Long> result;

        try {

            result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntryId, this.lastEntryId, this.bitSet);
            if(this.listState == LIST_STATE.EMPTY) {
                Assert.assertEquals(0, result.size());
            } else if (this.listState == LIST_STATE.NOT_EMPTY) {
                Assert.assertNotEquals(0, result.size());
            }

        }catch (Exception e) {
            Assert.assertTrue(isExpectedException);
        }

    }

}
