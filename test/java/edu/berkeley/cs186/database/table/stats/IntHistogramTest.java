package edu.berkeley.cs186.database.table.stats;

import edu.berkeley.cs186.database.StudentTest;
import edu.berkeley.cs186.database.StudentTestP2;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import edu.berkeley.cs186.database.datatypes.IntDataType;
import edu.berkeley.cs186.database.query.QueryPlan.PredicateOperator;

public class IntHistogramTest {

  @Test(timeout=1000)
  public void testSimpleHistogram() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i);
    }

    assertEquals(10, histogram.getEntriesInRange(0, 10));
  }

  @Test(timeout=1000)
  public void testComplexHistogram() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 40; i++) {
      histogram.addValue(i);
    }

    assertEquals(10, histogram.getEntriesInRange(0, 10));
    assertEquals(10, histogram.getEntriesInRange(10, 20));
    assertEquals(10, histogram.getEntriesInRange(20, 30));
    assertEquals(10, histogram.getEntriesInRange(30, 40));
    assertEquals(20, histogram.getEntriesInRange(20, 40));
    assertEquals(10, histogram.getEntriesInRange(15, 25));
    assertEquals(5, histogram.getEntriesInRange(25, 30));
  }

  @Test(timeout=1000)
  public void testHistogramExpand() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 10; i++) {
      histogram.addValue(i);
    }

    histogram.addValue(99);

    assertEquals(10, histogram.getAllBuckets().get(5).getCount());
    assertEquals(1, histogram.getAllBuckets().get(9).getCount());
  }

  @Test(timeout=1000)
  public void testComputeReductionFactor() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 50; i++) {
      histogram.addValue(i);
      histogram.addValue(i);
    }

    assertEquals(50, histogram.getNumDistinct());

    IntDataType equalsValue = new IntDataType(3);
    assertEquals(0.02f,
                 histogram.computeReductionFactor(PredicateOperator.EQUALS,
                                                  equalsValue),
                 0.001f);

    IntDataType lessThanValue = new IntDataType(25);
    assertEquals(0.5,
                 histogram.computeReductionFactor(PredicateOperator.LESS_THAN,
                                                  lessThanValue),
                 0.001f);

    IntDataType lessThanEqualsValue = new IntDataType(25);
    assertEquals(0.52,
                 histogram.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS,
                                                  lessThanEqualsValue),
                 0.001f);

    IntDataType greaterThanValue = new IntDataType(9);
    assertEquals(0.82,
                 histogram.computeReductionFactor(PredicateOperator.GREATER_THAN,
                                                  greaterThanValue),
                 0.001f);

    IntDataType greaterThanEqualsValue = new IntDataType(10);
    assertEquals(0.82,
                 histogram.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS,
                                                  greaterThanEqualsValue),
                 0.001f);
  }

  @Test(timeout=1000)
  public void testCopyWithReduction() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 100; i++) {
      histogram.addValue(i);
    }

    assertEquals(100, histogram.getNumDistinct());

    IntHistogram copyHistogram = histogram.copyWithReduction(0.7f);

    assertEquals(70, copyHistogram.getEntriesInRange(0, 100));
    assertEquals(70, copyHistogram.getNumDistinct());
  }

  @Test(timeout=1000)
  public void testCopyWithPredicate() {
    IntHistogram histogram = new IntHistogram();

    for (int i = 0; i < 500; i++) {
      histogram.addValue(i);
    }

    assertEquals(500, histogram.getNumDistinct());

    IntDataType value = new IntDataType(320);
    IntHistogram copyHistogram = histogram.copyWithPredicate(PredicateOperator.LESS_THAN,
                                                             value);

    assertEquals(320, copyHistogram.getEntriesInRange(0, 500));
    assertEquals(250, copyHistogram.getNumDistinct());
  }

  @Test@Category(StudentTestP2.class)
  public void testRangeCheckOps(){
      IntHistogram hist = new IntHistogram();

      for (int i = 0; i < 500; i++) {
          hist.addValue(i);
      }

      IntDataType val = new IntDataType(-1);

      assertEquals(0, hist.computeReductionFactor(PredicateOperator.EQUALS, val), .001f);
      assertEquals(1, hist.computeReductionFactor(PredicateOperator.NOT_EQUALS, val), .001f);
      assertEquals(1, hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val), .001f);
      assertEquals(0, hist.computeReductionFactor(PredicateOperator.LESS_THAN, val), .001f);
      assertEquals(1, hist.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS, val), .001f);
      assertEquals(0, hist.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS, val), .001f);

      IntDataType val2 = new IntDataType(1000);

      assertEquals(0, hist.computeReductionFactor(PredicateOperator.EQUALS, val2), .001f);
      assertEquals(1, hist.computeReductionFactor(PredicateOperator.NOT_EQUALS, val2), .001f);
      assertEquals(0, hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val2), .001f);
      assertEquals(1, hist.computeReductionFactor(PredicateOperator.LESS_THAN, val2), .001f);
      assertEquals(0, hist.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS, val2), .001f);
      assertEquals(1, hist.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS, val2), .001f);
  }

    @Test@Category(StudentTestP2.class)
    public void testConsistency() {
        IntHistogram hist = new IntHistogram();

        for (int i = 0; i < 500; i++) {
            hist.addValue(i);
        }

        IntDataType val = new IntDataType(20);

        assertEquals(1, hist.computeReductionFactor(PredicateOperator.EQUALS, val) +
                        hist.computeReductionFactor(PredicateOperator.NOT_EQUALS, val), .001f);

        IntDataType val1 = new IntDataType(50);
        IntDataType val2 = new IntDataType(49);

        assertNotEquals(hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val1),
                hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val2));
        assertNotEquals(hist.computeReductionFactor(PredicateOperator.LESS_THAN, val1),
                hist.computeReductionFactor(PredicateOperator.LESS_THAN, val2));

        assertNotEquals(hist.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS, val1),
                hist.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS, val2));
        assertNotEquals(hist.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS, val1),
                hist.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS, val2));

        IntDataType val3 = new IntDataType(250);

        assertEquals(hist.computeReductionFactor(PredicateOperator.LESS_THAN, val3) +
                        hist.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS, val3),
                (hist.computeReductionFactor(PredicateOperator.LESS_THAN, val3)*2)+
                        hist.computeReductionFactor(PredicateOperator.EQUALS, val3),
                .001f);

        assertEquals(hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val3) +
                        hist.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS, val3),
                (hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val3)*2)+
                        hist.computeReductionFactor(PredicateOperator.EQUALS, val3),
                .001f);
    }

    @Test@Category(StudentTestP2.class)
    public void testUsesLectureEquations() {
        IntHistogram hist = new IntHistogram();

        for (int i = 0; i < 500; i++) {
            hist.addValue(i);
        }

        IntDataType val1 = new IntDataType(100);
        IntDataType val2 = new IntDataType(200);
        IntDataType val3 = new IntDataType(300);
        IntDataType val4 = new IntDataType(400);

        assertEquals(1, hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val1) +
                        hist.computeReductionFactor(PredicateOperator.LESS_THAN, val1), .001f);
        assertEquals(1, hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val2) +
                        hist.computeReductionFactor(PredicateOperator.LESS_THAN, val2), .001f);
        assertEquals(1, hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val3) +
                        hist.computeReductionFactor(PredicateOperator.LESS_THAN, val3), .001f);
        assertEquals(1, hist.computeReductionFactor(PredicateOperator.GREATER_THAN, val4) +
                        hist.computeReductionFactor(PredicateOperator.LESS_THAN, val4), .001f);
    }

    @Test@Category(StudentTestP2.class)
    public void testOutOfBoundsCopy() {
        IntHistogram hist = new IntHistogram();
        IntHistogram emptyHist = new IntHistogram();

        for (int i = 0; i < 500; i++) {
            hist.addValue(i);
        }

        IntDataType val = new IntDataType(-100);
        IntHistogram newHist = hist.copyWithPredicate(PredicateOperator.GREATER_THAN, val);

        assertEquals(500, newHist.getNumDistinct());

        IntHistogram noValHist = hist.copyWithPredicate(PredicateOperator.LESS_THAN, val);

        assertEquals(0, noValHist.getNumDistinct());
        assertEquals(emptyHist.getNumDistinct(), noValHist.getNumDistinct());


    }

    @Test@Category(StudentTestP2.class)
    public void testEmptyHistOps() {
        IntHistogram emptyHist = new IntHistogram();

        IntDataType val;
        for (int i = 0; i < 10; i++) {
            val = new IntDataType(i);
            assertEquals(0, emptyHist.computeReductionFactor(PredicateOperator.EQUALS, val), .00f);
            assertEquals(0, emptyHist.computeReductionFactor(PredicateOperator.NOT_EQUALS, val), .00f);
            assertEquals(0, emptyHist.computeReductionFactor(PredicateOperator.LESS_THAN, val), .00f);
            assertEquals(0, emptyHist.computeReductionFactor(PredicateOperator.LESS_THAN_EQUALS, val), .00f);
            assertEquals(0, emptyHist.computeReductionFactor(PredicateOperator.GREATER_THAN, val), .00f);
            assertEquals(0, emptyHist.computeReductionFactor(PredicateOperator.GREATER_THAN_EQUALS, val), .00f);
        }
    }
}
