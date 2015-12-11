package org.sagebionetworks.bridge.synapse;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import com.google.common.collect.ImmutableList;
import org.sagebionetworks.client.SynapseClient;
import org.sagebionetworks.repo.model.table.QueryNextPageToken;
import org.sagebionetworks.repo.model.table.QueryResult;
import org.sagebionetworks.repo.model.table.QueryResultBundle;
import org.sagebionetworks.repo.model.table.Row;
import org.sagebionetworks.repo.model.table.RowSet;
import org.sagebionetworks.repo.model.table.SelectColumn;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class SynapseTableIteratorTest {
    private static final List<SelectColumn> DUMMY_HEADER_LIST = ImmutableList.of();
    private static final String TEST_SYNAPSE_TABLE_ID = "test-syn001";

    @DataProvider(name = "dataProvider")
    public Iterator<Object[]> dataProvider() {
        List<Object[]> testParamList = new ArrayList<>();
        for (int pageSize = 2; pageSize <= 3; pageSize++) {
            for (int numRows = 0; numRows <= 7; numRows++) {
                testParamList.add(new Object[] { numRows, pageSize });
            }
        }
        return testParamList.iterator();
    }

    @Test(dataProvider = "dataProvider")
    public void test(final int numRows, final int pageSize) throws Exception {
        // Set up test rows.
        Row[] rows = new Row[numRows];
        for (int i = 0; i < numRows; i++) {
            rows[i] = new Row();
            rows[i].setRowId((long) i);
        }

        // Calculate num pages.
        int numPages = numRows / pageSize;
        if (numRows % pageSize != 0) {
            // We have a remainder. Add an extra page.
            numPages++;
        }

        // Set up query pages.
        // Always have a page zero, for test purposes.
        QueryResult[] results = new QueryResult[Math.max(1, numPages)];
        {
            int i = 0;
            do {
                // Row list.
                List<Row> rowList = new ArrayList<>();
                for (int j = 0; j < pageSize; j++) {
                    int rowIdx = i*pageSize + j;
                    if (rowIdx < numRows) {
                        rowList.add(rows[rowIdx]);
                    }
                }

                // Row set object.
                RowSet rowSet = new RowSet();
                rowSet.setRows(rowList);
                rowSet.setEtag("etag" + i);
                rowSet.setHeaders(DUMMY_HEADER_LIST);

                // Query result.
                results[i] = new QueryResult();
                results[i].setQueryResults(rowSet);

                // Next page token, if this isn't the last page.
                int nextPage = i + 1;
                if (nextPage < numPages) {
                    QueryNextPageToken nextPageToken = new QueryNextPageToken();
                    nextPageToken.setToken("nextPageToken" + nextPage);
                    results[i].setNextPageToken(nextPageToken);
                }

                i++;
            } while (i < numPages);
        }

        // mock synapse client
        SynapseClient mockSynapseClient = mock(SynapseClient.class);

        when(mockSynapseClient.queryTableEntityBundleAsyncStart("SELECT * FROM " + TEST_SYNAPSE_TABLE_ID, null, null,
                true, SynapseClient.QUERY_PARTMASK, TEST_SYNAPSE_TABLE_ID)).thenReturn("asyncJob0");

        QueryResultBundle firstResultBundle = new QueryResultBundle();
        firstResultBundle.setQueryResult(results[0]);
        when(mockSynapseClient.queryTableEntityBundleAsyncGet("asyncJob0", TEST_SYNAPSE_TABLE_ID))
                .thenReturn(firstResultBundle);

        for (int i = 1; i < numPages; i++) {
            when(mockSynapseClient.queryTableEntityNextPageAsyncStart("nextPageToken" + i, TEST_SYNAPSE_TABLE_ID))
                    .thenReturn("asyncJob" + i);
            when(mockSynapseClient.queryTableEntityNextPageAsyncGet("asyncJob" + i, TEST_SYNAPSE_TABLE_ID))
                    .thenReturn(results[i]);
        }

        // set up iterator, execute, and validate
        SynapseTableIterator tableIter = new SynapseTableIterator(mockSynapseClient, null, TEST_SYNAPSE_TABLE_ID);
        for (int i = 0; i < numRows; i++) {
            assertTrue(tableIter.hasNext());
            // extra call to hasNext() just to make sure it doesn't advance the iterator
            assertTrue(tableIter.hasNext());
            assertEquals(tableIter.next().getRowId().longValue(), i);

            // getEtag() should always return etag0, which is etag of the first page
            assertEquals(tableIter.getEtag(), "etag0");

            // check that headers is set
            assertSame(tableIter.getHeaders(), DUMMY_HEADER_LIST);
        }

        // End of stream. hasNext() returns false. next() throws. getEtag() still returns etag of the first page.
        // getHeaders() still returns headers
        assertFalse(tableIter.hasNext());
        assertEquals(tableIter.getEtag(), "etag0");
        assertSame(tableIter.getHeaders(), DUMMY_HEADER_LIST);
        Exception thrownEx = null;
        try {
            tableIter.next();
            fail("expected exception");
        } catch (NoSuchElementException ex) {
            thrownEx = ex;
        }
        assertNotNull(thrownEx);
    }
}
