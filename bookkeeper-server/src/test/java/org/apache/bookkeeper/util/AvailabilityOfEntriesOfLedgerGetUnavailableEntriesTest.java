package org.apache.bookkeeper.util;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesOfLedgerGetUnavailableEntriesTest {

    private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;
    private long startEntryId;
    private long lastEntryId;
    private BITSET_STATE bitsetState;
    private LIST_STATE listState;
    private boolean isExpectedException;
    private boolean nullable;
    private boolean sequence;
    private BitSet validBitset;
    private BitSet invalidBitset;
    private long[] entries;

    public AvailabilityOfEntriesOfLedgerGetUnavailableEntriesTest(

            long startEntryId,
            long lastEntryId,
            BITSET_STATE bitsetState,
            LIST_STATE listState,
            boolean isExpectedException,
            boolean nullable,
            boolean sequence) {

            this.startEntryId = startEntryId;
            this.lastEntryId = lastEntryId;
            this.bitsetState = bitsetState;
            this.listState = listState;
            this.isExpectedException = isExpectedException;
            this.nullable = nullable;
            this.sequence = sequence;
    }

    private void creations() {

        long[] sequenceEntries = new long[]{1L, 3L, 5L, 7L, 9L, 11L, 13L, 15L};
        this.entries = new long[]{3L, 5L, 7L, 9L, 11L, 13L};
        long[] invalidEntries = new long[]{2L, 5L, 8L, 9L, 11L, 16L};

        if(this.sequence){
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(sequenceEntries);
        } else if (this.nullable) {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger((long[]) null);
        } else {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.entries);
        }

        this.validBitset = new BitSet(16);
        this.invalidBitset = new BitSet(16);

        for(long entryId : this.entries) {
            this.validBitset.set((int) entryId);
        }

        for(long entryId : invalidEntries) {
            this.invalidBitset.set((int) entryId);
        }

    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        return Arrays.asList(new Object[][] {
                //{startEntryID,        lastEntryId,    BITSET_STATE,           LIST_STATE,             EXCEPTION,      SEQUENCE        NULLABLE}
                {-1L,                   -2L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       true,           false,          false},     //case 1
                {-1L,                   -1L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       true,           false,          false},     //case 2
                { 0L,                   0L,             BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,          false},     //case 3
                { 0L,                   1L,             BITSET_STATE.INVALID,   LIST_STATE.EMPTY,       false,          false,          false},     //case 4
                { 0L,                   0L,             BITSET_STATE.INVALID,   LIST_STATE.EMPTY,       false,          false,          false},     //case 5
                { 0L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,          false},     //case 6
                { 0L,                   15L,            BITSET_STATE.INVALID,   LIST_STATE.NOT_EMPTY,   false,          false,          false},     //case 7
                { 0L,                   15L,            BITSET_STATE.NULL,      LIST_STATE.NOT_EMPTY,   true,           false,          false},     //case 8
                { 0L,                   0L,             BITSET_STATE.NULL,      LIST_STATE.EMPTY,       true,           false,          false},     //case 9
                {-1L,                   0L,             BITSET_STATE.NULL,      LIST_STATE.EMPTY,       true,           false,          false},     //case 10
                {15L,                   0L,             BITSET_STATE.VALID,     LIST_STATE.EMPTY,       true,           false,          false},     //case 11
                {15L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,          false},     //case 12
                {15L,                   0L,             BITSET_STATE.NULL,      LIST_STATE.EMPTY,       true,           false,          false},     //case 13
                {15L,                   16L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,          false},     //case 14
                { 0L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       false,          false,          true},      //case 15
                { 0L,                   15L,            BITSET_STATE.VALID,     LIST_STATE.EMPTY,       true,           true,           false}      //case 16
        });
    }

    @Test
    public void getUnavailableEntriesTest() {

        List<Long> result;

        try {
            creations();

            if (this.sequence && this.bitsetState == BITSET_STATE.VALID) {

                result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntryId, this.lastEntryId, this.validBitset);
                if(this.listState == LIST_STATE.EMPTY) {
                    Assert.assertEquals(0, result.size());
                }else if(this.listState == LIST_STATE.NOT_EMPTY){
                    Assert.assertNotEquals(0, result.size());
                }

            } else if(this.bitsetState == BITSET_STATE.VALID) {

                result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntryId, this.lastEntryId, this.validBitset);
                if(this.listState == LIST_STATE.EMPTY) {
                    Assert.assertEquals(0, result.size());
                }else if(this.listState == LIST_STATE.NOT_EMPTY){
                    Assert.assertNotEquals(0, result.size());
                }

            } else if (this.bitsetState == BITSET_STATE.INVALID) {

                result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntryId, this.lastEntryId, this.invalidBitset);
                if(this.listState == LIST_STATE.EMPTY) {
                    Assert.assertEquals(0, result.size());
                }else if(this.listState == LIST_STATE.NOT_EMPTY){
                    Assert.assertNotEquals(0, result.size());
                }

            } else if (this.bitsetState == BITSET_STATE.NULL) {

                result = this.availabilityOfEntriesOfLedger.getUnavailableEntries(this.startEntryId, this.lastEntryId, null);
                if(this.listState == LIST_STATE.EMPTY) {
                    Assert.assertEquals(0, result.size());
                }else if(this.listState == LIST_STATE.NOT_EMPTY){
                    Assert.assertNotEquals(0, result.size());
                }

            }

        } catch (Exception e){
            Assert.assertTrue(this.isExpectedException);
        }

    }

}
