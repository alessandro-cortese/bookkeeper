package org.apache.bookkeeper.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class AvailabilityOfEntriesIsEntryAvailableTest {

        private AvailabilityOfEntriesOfLedger availabilityOfEntriesOfLedger;
    private boolean expectedResult;
    private boolean expectedFailure;
    private boolean nullable;
    private long entry;
    private long[] entries;

    public AvailabilityOfEntriesIsEntryAvailableTest(

            long entry,
            boolean expectedResult,
            boolean expectedFailure,
            boolean nullable) {

            this.entry = entry;
            this.expectedResult = expectedResult;
            this.expectedFailure = expectedFailure;
            this.nullable = nullable;

    }

    @Before
    public void setUp() {

        this.entries = new long[]{-1L, 1L, 3L, 5L, 7L, -6L, 9L, -10L, 11L, 15L, 17L, -19L};
        if(!this.nullable) {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(this.entries);
        }else {
            this.availabilityOfEntriesOfLedger = new AvailabilityOfEntriesOfLedger(new long[]{});
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {

        return Arrays.asList(new Object[][]{
                //{ENTRY_ID,    EXPECTED_RESULT,    FAILURE,    NULLABLE}
                {-1L,           false,              false,      false},     //case 1
                {3L,            true,               false,      false},     //case 2
                {4L,            false,              false,      false},     //case 3
                {5L,            true,               false,      false},     //case 4
                {6L,            false,              false,      false},     //case 5
                {-6L,           false,              true,       false},     //case 6    <-- Failure
                {11L,           true,               false,      false},     //case 7
                {15L,           true,               false,      false},     //case 8
                {16L,           false,              false,      false},     //case 9
                {-19L,          false,              true,       false},     //case 10   <-- Failure
                {11L,           false,              false,      true}       //case 11
        });

    }

    @Test
    public void testIsEntryAvailable() {
        boolean result = availabilityOfEntriesOfLedger.isEntryAvailable(this.entry);
        if(!this.expectedFailure) {
            Assert.assertEquals(expectedResult, result);
        }else {
            Assert.assertEquals(expectedResult, !result);
        }
    }

}
