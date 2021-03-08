package org.coala.planner;

import org.cardygan.util.AssertUtil;
import org.coala.model.PerformanceInfluenceTerm;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class LogNormalizer_old {

	private final List<PerformanceInfluenceTerm> featureInteractions;
	private List<PerformanceInfluenceTerm> normalizedFeatureInteractions = null;
	private static double NEW_MAX_WEIGHT = 1000d;
	private static double NEW_MIN_WEIGHT = 0.1d;
	private static int DIVISION_SCALE = 15;

	/**
	 * Creates new LogNormalizer.
	 *
	 * @param featureInteractions
	 *            List of feature interactions
	 */
	public LogNormalizer_old(List<PerformanceInfluenceTerm> featureInteractions) {
		this.featureInteractions = featureInteractions;
	}

	/**
	 * Normalize by x' = newMinWeight + (x - x_min)/(x_max - x_min) (newMaxWeight -
	 * newMinWeight)
	 */
	void normalize() {
		double x_min = getMinWeight(featureInteractions);
		// AssertUtils.trueOrFail("Minimum absolute weight is no allowed to be
		// 0.", minFactor != 0);
		double x_max = getMaxWeight(featureInteractions);

		BigDecimal minVal = null;
		Map<PerformanceInfluenceTerm, BigDecimal> coeffs = new HashMap<>();

		for (PerformanceInfluenceTerm fi : featureInteractions) {
			double x = fi.getWeight();

			BigDecimal nominator = BigDecimal.valueOf(x).subtract(BigDecimal.valueOf(x_min));
			BigDecimal demoninator = BigDecimal.valueOf(x_max).subtract(BigDecimal.valueOf(x_min));

			BigDecimal coeff;
			if (nominator.compareTo(demoninator) == 0) {
				//TODO was BigDecimal.ZERO
				coeff = BigDecimal.ONE;
			} else {
				coeff = nominator.divide(demoninator, DIVISION_SCALE, RoundingMode.HALF_UP);
			}
			coeffs.put(fi, coeff);

			if (minVal == null || minVal.compareTo(coeff) < 0) {
				minVal = coeff;
			}
		}

		BigDecimal newMaxWeight;

		if (minVal.compareTo(BigDecimal.ZERO) == 0) {
			newMaxWeight = BigDecimal.valueOf(NEW_MAX_WEIGHT);
		} else {
			newMaxWeight = BigDecimal.valueOf(Double.MIN_VALUE).divide(minVal, DIVISION_SCALE, RoundingMode.HALF_UP);
			// this is always true?
			if (newMaxWeight.compareTo(BigDecimal.ONE) <= NEW_MAX_WEIGHT) {
				newMaxWeight = BigDecimal.valueOf(NEW_MAX_WEIGHT);
			}
		}

		List<PerformanceInfluenceTerm> ret = new ArrayList<>(featureInteractions.size());
		for (PerformanceInfluenceTerm fi : featureInteractions) {

			BigDecimal multiplier = newMaxWeight.subtract(BigDecimal.valueOf(NEW_MIN_WEIGHT));
			double x_new = NEW_MIN_WEIGHT + coeffs.get(fi).multiply(multiplier).doubleValue();

			AssertUtil.trueOrFail("New weight must have a valid double value.", !Double.isNaN(x_new));

			ret.add(new PerformanceInfluenceTerm(x_new, fi.getFeatures(), fi.getAttributes()));
		}
		normalizedFeatureInteractions = ret;
	}

	double getMaxWeight(List<PerformanceInfluenceTerm> featureInteractions) {
		return featureInteractions.stream().mapToDouble(fi -> fi.getWeight()).max().getAsDouble();
	}

	/**
	 * Normalize weights of feature interactions such that lowest weight is equal to
	 * <code>minimumWeight</code>.
	 *
	 *
	 * @return List of normalized feature interactions
	 */
	public List<PerformanceInfluenceTerm> getNormalizedInteractions() {
		if (normalizedFeatureInteractions == null) {
			normalize();
		}

		AssertUtil.trueOrFail("After normalization feature interactions should not be null",
				normalizedFeatureInteractions != null);

		return normalizedFeatureInteractions;
	}

	/**
	 * Calculates translation constant x from feature interactions such that
	 * x=1-min(Y)
	 */
	public double getTranslationConstant() {
		if (normalizedFeatureInteractions == null) {
			normalize();
		}

		AssertUtil.trueOrFail("After normalization feature interactions should not be null",
				normalizedFeatureInteractions != null);

		double minY = getMinWeight(normalizedFeatureInteractions);
		return 1 - minY >= 1 ? 1 - minY : 1;
	}

	double getMinWeight(List<PerformanceInfluenceTerm> featureInteractions) {
		return featureInteractions.stream().mapToDouble(fi -> fi.getWeight()).min().getAsDouble();
	}

}
