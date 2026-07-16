package smart

import (
	"math"
	"testing"

	"github.com/metacubex/mihomo/common/lru"
)

func almostEqual(a, b float64) bool {
	const epsilon = 1e-12
	return math.Abs(a-b) <= epsilon
}

func TestUpdateIIDRewardUsesDiscountedMeanAndCount(t *testing.T) {
	record := &AtomicStatsRecord{
		weights: lru.New[string, float64](lru.WithSize[string, float64](16)),
	}

	rewards := []float64{0.2, 0.8, 0.5}
	var expectedMean float64
	var expectedCount float64

	for i, reward := range rewards {
		discountedCount := expectedCount * DiscountedUCBGamma
		expectedCount = discountedCount + 1.0
		expectedMean = (expectedMean*discountedCount + reward) / expectedCount

		mean, count := record.UpdateIIDReward(IIDRewardTypeTCP, reward)
		if !almostEqual(mean, expectedMean) {
			t.Fatalf("step %d mean = %.12f, want %.12f", i, mean, expectedMean)
		}
		if !almostEqual(count, expectedCount) {
			t.Fatalf("step %d count = %.12f, want %.12f", i, count, expectedCount)
		}

		storedMean, storedCount := record.GetIIDReward(IIDRewardTypeTCP)
		if !almostEqual(storedMean, expectedMean) {
			t.Fatalf("step %d stored mean = %.12f, want %.12f", i, storedMean, expectedMean)
		}
		if !almostEqual(storedCount, expectedCount) {
			t.Fatalf("step %d stored count = %.12f, want %.12f", i, storedCount, expectedCount)
		}
	}
}

func TestCalculateUCB1TunedScoreUsesDiscountedUCB(t *testing.T) {
	mean := 0.7
	count := 2.5
	totalCount := 10.0

	score, bonus := CalculateUCB1TunedScore(mean, count, totalCount)
	expectedBonus := math.Sqrt(2.0 * math.Log(totalCount) / count)
	expectedScore := mean + expectedBonus

	if !almostEqual(bonus, expectedBonus) {
		t.Fatalf("bonus = %.12f, want %.12f", bonus, expectedBonus)
	}
	if !almostEqual(score, expectedScore) {
		t.Fatalf("score = %.12f, want %.12f", score, expectedScore)
	}
}

func TestCalculateUCB1TunedScoreAdjustsTotalCountAndClampsMean(t *testing.T) {
	mean := 1.5
	count := 5.0
	totalCount := 3.0

	score, bonus := CalculateUCB1TunedScore(mean, count, totalCount)
	expectedBonus := math.Sqrt(2.0 * math.Log(count) / count)
	expectedScore := 1.0 + expectedBonus

	if !almostEqual(bonus, expectedBonus) {
		t.Fatalf("bonus = %.12f, want %.12f", bonus, expectedBonus)
	}
	if !almostEqual(score, expectedScore) {
		t.Fatalf("score = %.12f, want %.12f", score, expectedScore)
	}
}

func TestCalculateUCB1TunedScoreHandlesColdAndSingleSampleInputs(t *testing.T) {
	score, bonus := CalculateUCB1TunedScore(0.5, 0, 10)
	if !math.IsInf(score, 1) || !math.IsInf(bonus, 1) {
		t.Fatalf("cold arm score/bonus = %.12f/%.12f, want +Inf/+Inf", score, bonus)
	}

	score, bonus = CalculateUCB1TunedScore(1.5, 1, 1)
	if score != 1.0 || bonus != 0.0 {
		t.Fatalf("single-sample score/bonus = %.12f/%.12f, want 1/0", score, bonus)
	}
}
