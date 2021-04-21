package org.coala.planner;

import org.cardygan.config.*;
import org.cardygan.config.util.ConfigUtil;
import org.cardygan.fm.*;
import org.cardygan.ilp.api.*;
import org.cardygan.ilp.api.CplexSolver.CplexSolverBuilder;
import org.cardygan.ilp.api.expr.ArithExpr;
import org.cardygan.ilp.api.expr.Var;
import org.cardygan.ilp.api.expr.bool.BoolExpr;
import org.coala.event.EventBus;
import org.coala.event.MilpSolverFinishedEvent;
import org.coala.model.AttributeScaling;
import org.coala.model.PerformanceInfluenceTerm;
import org.coala.util.FmUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tcep.graph.transition.mapek.contrast.FmNames;

import java.io.File;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.cardygan.ilp.api.util.ExprDsl.*;
import static org.cardygan.util.AssertUtil.trueOrFail;
import static org.coala.util.IlpUtils.addConstraint;
import static org.coala.util.IlpUtils.addObjective;

//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;

public class IlpBasedPlanner implements Planner {
	private final EventBus bus;

	public IlpBasedPlanner(EventBus bus) {
		this.bus = bus;
	}

	public static final String METRIC_NAME_ILP_OBJVALUE = "ILP Objective Value";

	private final static String CPLEX_LIB_PATH = "CPLEX_LIB_PATH";

	private Logger logger = LoggerFactory.getLogger(getClass());

	private boolean writeModelFile = true;

	@Override
	public Config plan(final FM fm, final Config config, final List<PerformanceInfluenceTerm> featureInteractions,
					   final AttributeScaling scaling, final Optional<Integer> randomSeed, final Optional<File> modelFile) {
		if (System.getenv(CPLEX_LIB_PATH) == null) {
			throw new IllegalStateException(
					"Environment variable " + CPLEX_LIB_PATH + " not set. Path must point to cplex library path "
							+ "(e.g., /Applications/IBM/ILOG/CPLEX_Studio1263/cplex/bin/x86-64_osx/).");
		}

		try {
			Class.forName("ilog.concert.IloException");
		} catch (ClassNotFoundException e) {
			throw new IllegalStateException("Could not find cplex library on classpath.");
		}

		Model ilp = Fm2IlpTransformer.transform(fm, scaling);
		List<AttrInst> allAttributeInst = ConfigUtil.getAttrInstances(config, config.getRoot());
		List<AttrInst> allContextAttributeInst = ConfigUtil.getAttrInstances(config, ConfigUtil.getFeatureInstanceByName(config.getRoot(), FmNames.CONTEXT()));
		List<AttrInst> allSystemAttributeInst = ConfigUtil.getAttrInstances(config, ConfigUtil.getFeatureInstanceByName(config.getRoot(), FmNames.SYSTEM()));

		if (!featureInteractions.isEmpty()) {
			// Handle soft constraints
			int fiCounter = 0;
			List<ArithExpr> objVars = new ArrayList<>();

			/**
			 * @author Niels
			 * changed so that numeric features (attributes) are resolved by multiplying them into the term weights before calling the solver
			 * -> avoid normalization + log-transformation for including attributes into the ILP model
			 * drawback: numeric features (attributes) only allowed for context part of the CFM, NOT IN SYSTEM PART
			 * (i.e. only binary features allowed as reconfigurable system elements)
			 *
			 * Reason for this change: log-normalization was used to transform non-linear problem into linear problem (e.g. f_a * f_a -> log(f_a) + log(f_a) ),
			 * but LogNormalizer was bugged and result depended on choice of lower/upper bounds for normalization (which we could not supply a good value for).
			 * So instead of log-transforming numeric feature values they are multiplied into the term weight, bypassing the normalization + log-transformation
			 */
			//LogNormalizer normalizer = new LogNormalizer(featureInteractions);
			//List<PerformanceInfluenceTerm> normalizedFeatureInteractions = normalizer.getNormalizedInteractions();
			//double transConst = normalizer.getTranslationConstant();
			//trueOrFail("Translation factor is expected to be greater or equal than 1: ", transConst >= 1);

			Map<Attribute, DoubleVar> helpingVars = new HashMap<>();

			//for (PerformanceInfluenceTerm fi : normalizedFeatureInteractions) {
			for (PerformanceInfluenceTerm fi : featureInteractions) {
				List<ArithExpr> rhs = new ArrayList<>();

				double weight_acc = fi.getWeight();

				// presence condition from binary features
				List<BoolExpr> presCond = new ArrayList<>();
				for (Feature f : fi.getFeatures().keySet()) {
					BinaryVar f_v = FmUtils.getVar(ilp, f);
					presCond.add(f_v);
				}
				// resolve attributes
				for (Entry<Attribute, Integer> entry : fi.getAttributes().entrySet()) {
					Attribute attr = entry.getKey();

					Integer occurences = entry.getValue();

					if(occurences > 1 && allSystemAttributeInst.stream().map(i -> i.getName()).anyMatch(name -> name.equals(attr.getName()))) {
						throw new IllegalStateException("Performance Influence Model must not contain numeric features from the system side of the CFM!");
					}

					// helping variable a_h
					Optional<Var> a_h_Search = ilp.getVars().stream()
							.filter(v -> v.getName().equals(attr.getName() + "_h")).findAny();
					DoubleVar a_h = a_h_Search.isPresent() ? (DoubleVar) a_h_Search.get()
							: ilp.newDoubleVar(attr.getName() + "_h");

					helpingVars.put(entry.getKey(), a_h);

					// parent feature of attribute
					BinaryVar f_p = FmUtils.getVar(ilp, (Feature) attr.eContainer());
					presCond.add(f_p);

					// retrieve scaling factor
					int scalingFactor = scaling.hasScaling(attr) ? scaling.getScaling(attr) : 1;

					// bind a_h to f_p
					final double l;
					final double u;
					final double val;
					if (attr.getDomain() instanceof Int) {
						l = ((Int) attr.getDomain()).getBounds().getLb();
						u = ((Int) attr.getDomain()).getBounds().getUb();
						val = (double) ((IntAttrInst) allAttributeInst.stream().filter(a -> a.getName().equals(attr.getName())).findFirst().get()).getVal() * scalingFactor;
					} else if (attr.getDomain() instanceof Real) {
						l = ((Real) attr.getDomain()).getBounds().getLb();
						u = ((Real) attr.getDomain()).getBounds().getUb();
						val = ((RealAttrInst) allAttributeInst.stream().filter(a -> a.getName().equals(attr.getName())).findFirst().get()).getVal() * scalingFactor;
					} else {
						throw new IllegalStateException("Unknown attribute type. Could not retrieve domain bounds.");
					}

					trueOrFail("Bound are expected to be greater or equal than 0", l >= 0 && u >= 0 && u >= l);

					// a = 1 - min(Y)
					// f_p -> log(l+a) <= a' <= log(u+a)

					//trueOrFail("Cannot log negative value: " + l * scalingFactor + transConst,
					//		l * scalingFactor + transConst >= 1);
					//double lb = Math.log(l * scalingFactor + transConst);

					//trueOrFail("Cannot log negative value: " + u * scalingFactor + transConst,
					//		u * scalingFactor + transConst >= 1);
					//double ub = Math.log(u * scalingFactor + transConst);
					addConstraint(IlpBasedPlanner.class, ilp, "Binding f_p to a' (1): ",
							"bind_" + f_p.getName() + "To" + attr.getName() + "_t",
							impl(f_p, and(leq(p(l * scalingFactor), a_h), leq(a_h, p(u * scalingFactor)))));

					// !f_p -> a' = 0
					addConstraint(IlpBasedPlanner.class, ilp, "Binding f_p to a' (2): ",
							"bind_" + f_p.getName() + "To" + attr.getName() + "_f", impl(not(f_p), eq(p(0), a_h)));

					if(allContextAttributeInst.stream().map(i -> i.getName()).anyMatch(name -> name.equals(attr.getName()))) {
						// multiply context attribute value onto the term weight
						for (int i = 0; i < occurences; i++)
							weight_acc = weight_acc * val;
					}

					//rhs.add(mult(p(negated_occurences), a_h));
				}

				// add constant w

				//double transformedCost = fi.getWeight() + transConst;

				//trueOrFail("Problem in translation leading to log of weight that is less than 1: log(" + fi.getWeight()
				//		+ "+" + transConst + ")", transformedCost >= 1);

				//double negated_Const = -1.0 * Math.log(transformedCost);
				//rhs.add(p(negated_Const));

				/**
				 * @author Niels
				 * use negated weight to minimize instead of maximize objective function
 				 */
				rhs.add(p(-1.0 * weight_acc));

				double count = fiCounter++;
				DoubleVar w_i = ilp.newDoubleVar("w_" + count);
				objVars.add(w_i);

				ArithExpr s = sum(rhs);

				addConstraint(IlpBasedPlanner.class, ilp, "Adding soft constraint 1: ", "w_" + count + "_t",
						impl(and(presCond), geq(sum(rhs), w_i)));

				addConstraint(IlpBasedPlanner.class, ilp, "Adding soft constraint 2: ", "w_" + count + "_f",
						impl(not(and(presCond)), eq(p(0), w_i)));

			}

			// Add constraints to fix context features
			if (config != null && config.getRoot() != null) {
				Set<Instance> fixedLeafs = ConfigUtil.getLeafs(config, config.getRoot());
				for (Feature f : fixedLeafs.stream().map(i -> i.getType()).collect(Collectors.toList())) {
					Var fixedVar = FmUtils.getVar(ilp, f);

					addConstraint(IlpBasedPlanner.class, ilp, "Adding constraint: fix context feature ",
							"fixed_" + fixedVar.getName(), geq(fixedVar, p(1)));
				}

				// Add constraints to fix context attributes
				List<AttrInst> fixedAttrs = ConfigUtil.getAttrInstances(config, config.getRoot());
				for (AttrInst a : fixedAttrs) {
					Var fixedVar = FmUtils.getVar(ilp, a.getType());

					double val = 1;
					if (a instanceof IntAttrInst) {
						val = ((IntAttrInst) a).getVal();
					} else if (a instanceof RealAttrInst) {
						val = ((RealAttrInst) a).getVal();
					} else {
						throw new IllegalStateException("Not supported attribute type.");
					}
					addConstraint(IlpBasedPlanner.class, ilp, "Adding constraint: fix context attribute ",
							"fixed_" + fixedVar.getName(), eq(fixedVar, p(val)));

					// add constraints for fixed helping variables
					if (!featureInteractions.isEmpty() && helpingVars.containsKey(a.getType())) {
						DoubleVar a_h = helpingVars.get(a.getType());

						// retrieve scaling factor
						int scalingFactor = scaling.hasScaling(a.getType()) ? scaling.getScaling(a.getType()) : 1;

						//trueOrFail("Cannot log negative value: " + val * scalingFactor + transConst,
						//		val * scalingFactor + transConst >= 1);
						//double val_h = Math.log(val * scalingFactor + transConst);

						addConstraint(IlpBasedPlanner.class, ilp, "Adding constraint: fix context helping attribute ",
								"fixed_" + a_h.getName(), eq(a_h, p(val * scalingFactor)));
					}
				}
			}
			// minimize instead of maximize objective by using negated weights
			// TODO check if max parameter works as intended
			addObjective(IlpBasedPlanner.class, ilp, "Adding objective: ", true, sum(objVars));
		}
		logger.debug("pre cplexSolver creation");
		CplexSolverBuilder basicCplexBuilder = CplexSolver.create().withPresolve().withLogging(true);
		logger.debug("cplexSolver created");
		if (randomSeed.isPresent()) {
			basicCplexBuilder = basicCplexBuilder.withSeed(randomSeed.get());
		}

		if (this.writeModelFile && modelFile.isPresent()) {
			basicCplexBuilder.withModelOutput(modelFile.get().getAbsolutePath());
		}

		logger.debug("calling CPLEX with the following ilp: \n" + ilp.toString());

		final CplexSolver solver = basicCplexBuilder.build();

		logger.info("built CPLEX solver \n" + solver.toString());

		try {
			final Result res = ilp.solve(solver);

			logger.info("Solver result is " + res.toString());

			final long durationNanoSecs = TimeUnit.NANOSECONDS.convert(res.getStatistics().getDuration(),
					TimeUnit.MILLISECONDS);

			bus.fireEvent(new MilpSolverFinishedEvent(durationNanoSecs, res.getObjVal()));

			final Config finalConfiguration = FmUtils.getConfig(fm, scaling, res);
			return finalConfiguration;
		} catch(Throwable e) {
			logger.error("PROBLEM WHILE SOLVING ILP with CPLEX: \n" + e.toString() );
			throw e;
		}
	}

}
