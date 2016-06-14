package algorithms;

import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import controller.GeneticAlgorithmInterface;

public class MicrobialAlgorithm {
	static Random rd = new Random();

	/*
	 * @param: D, deme size
	 * 
	 * @param: REC, recombination rate
	 * 
	 * @param: MUT, mutation rate
	 */
	public static void microbial_tournament(int D, double REC, double MUT,
			double[][] gene, double[] fittnessScores) {
		microbial_tournament(D, REC, MUT, gene, fittnessScores, true);
	}

	/*
	 * @param: D, deme size
	 * 
	 * @param: REC, recombination rate
	 * 
	 * @param: MUT, mutation rate
	 * 
	 * @param: smallLessHealthy, smaller fitness score indicated less healthy
	 */
	public static void microbial_tournament(int D, double REC, double MUT,
			double[][] gene, double[] fittnessScores, boolean smallLessHealthy) {
		int P = gene.length;
		int N = gene[0].length;

		int A = (int) (P * rd.nextDouble());
		int B = (A + 1 + (int) (D * rd.nextDouble())) % P;

		int W, L;
		// if (eval(A, gene) > eval(B, gene)) {
		if ((smallLessHealthy && fittnessScores[A] > fittnessScores[B])
				|| (!smallLessHealthy && fittnessScores[A] < fittnessScores[B])) {
			W = A;
			L = B;
		} else {
			W = B;
			L = A;
		}

		for (int i = 0; i < N; i++) {
			boolean changed = false;
			if (rd.nextDouble() < REC) {
				gene[L][i] = gene[W][i];
				changed = true;
			}

			if (rd.nextDouble() < MUT) {
				if (i == 0) {
					gene[L][i] = rd.nextInt(GeneticAlgorithmInterface
							.getSoilTypes().length)
							% GeneticAlgorithmInterface.getSoilTypes().length;
				} else if (i == 1) {
					gene[L][i] += rd.nextGaussian();

					if (gene[L][i] < 0) {
						gene[L][i] = 0;
					} else if (gene[L][i] > 1) {
						gene[L][i] = 1;
					}
				} else if (i == 3) {
					gene[L][i] = rd.nextInt();
				} else {
					gene[L][i] += rd.nextGaussian() * 0.167;

					if (gene[L][i] <= 0) {
						gene[L][i] = 0.01;
					} else if (gene[L][i] > 0.167) {
						gene[L][i] = 0.167;
					}
				}

				changed = true;
			}

			if (changed) {
				fittnessScores[L] = eval(L, gene);
			}
		}
	}

	/*
	 * fitness evaluation function
	 */
	public static double eval(int A, double[][] gene) {
		double[] breaks = { 0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000,
				9000 };
		int[] counts = { 1015, 714, 496, 166, 134, 63, 40, 16, 26, 4 };
		int totalCount = 0;
		for (int n : counts) {
			totalCount += n;
		}

		double[] dist = new double[counts.length];
		for (int i = 0; i < counts.length; i++) {
			dist[i] = counts[i] * 1.0 / totalCount;
		}

		return evalKLDivergence(A, gene, 2011, dist, breaks);
	}

	public static double evalAvgYield(int A, double[][] gene, int year,
			double targetYield) {
		// int year = 2011;
		// double targetYield = 1826;

		int soilIndex = (int) gene[A][0];
		double localToHybridRatio = gene[A][1];
		double pDayStd = gene[A][2];
		int randomSeed = (int) gene[A][3];

		try {
			GeneticAlgorithmInterface.initializeModel(localToHybridRatio,
					soilIndex, pDayStd, year, randomSeed);
			GeneticAlgorithmInterface.runSim();
			double avgYield = GeneticAlgorithmInterface.getAvgYield(year,
					GeneticAlgorithmInterface.getSoilTypes()[soilIndex]);
			return 1 - Math.abs(targetYield - avgYield) / targetYield;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public static double evalSd(int A, double[][] gene, int year,
			double targetSd) {
		// int year = 2011;
		// double targetSd = 1647;

		int soilIndex = (int) gene[A][0];
		double localToHybridRatio = gene[A][1];
		double pDayStd = gene[A][2];
		int randomSeed = (int) gene[A][3];

		try {
			GeneticAlgorithmInterface.initializeModel(localToHybridRatio,
					soilIndex, pDayStd, year, randomSeed);
			GeneticAlgorithmInterface.runSim();
			double sd = GeneticAlgorithmInterface.getYieldStandardDev(year,
					GeneticAlgorithmInterface.getSoilTypes()[soilIndex]);
			return 1 - Math.abs(targetSd - sd) / targetSd;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return 0;
	}

	public static double evalKLDivergence(int A, double[][] gene, int year,
			double[] distribution, double[] breaks) {
		int soilIndex = (int) gene[A][0];
		double localToHybridRatio = gene[A][1];
		double pDayStd = gene[A][2];
		int randomSeed = (int) gene[A][3];

		try {
			GeneticAlgorithmInterface.initializeModel(localToHybridRatio,
					soilIndex, pDayStd, year, randomSeed);
			GeneticAlgorithmInterface.runSim();
			double klDiv = GeneticAlgorithmInterface.getKLDivergence(year,
					GeneticAlgorithmInterface.getSoilTypes()[soilIndex],
					distribution, breaks);
			return klDiv;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return Double.MAX_VALUE;
	}

	public static void main(String[] args) {
		/*
		if (args.length < 3) {
			System.out
					.println("Usage: runGA.sh populationSize demeSize numOfGenerations");
			return;
		}
		*/
		int popSize = 10;
				//Integer.parseInt(args[0]);
		int demeSize = 5;
		//Integer.parseInt(args[1]);
		int numOfGenerations = 5;
		//Integer.parseInt(args[2]);
		boolean smallLessHealthy = false;

		// Remember the start time
		long startTime = new Date().getTime();

		String[] soilTypes = GeneticAlgorithmInterface.getSoilTypes();
		double[][] gene = new double[popSize][4];
		// initialization
		for (int i = 0; i < gene.length; i++) {
			gene[i][0] = rd.nextInt(soilTypes.length); // soilType
			gene[i][1] = rd.nextDouble(); // local to hybrid maize ratio
			gene[i][2] = 0.001 + rd.nextDouble() * (0.167 - 0.001); // std dev
																	// 0.001 -
																	// 0.167
			gene[i][3] = rd.nextInt();
		}

		double[] fittnessScores = new double[gene.length];
		for (int i = 0; i < fittnessScores.length; i++) {
			fittnessScores[i] = eval(i, gene);
		}

		for (int i = 0; i < numOfGenerations; i++) {
			double highest_score = 0;
			double lowest_score = Double.MAX_VALUE;
			int highest_index = 0, lowest_index = 0;
			for (int j = 0; j < fittnessScores.length; j++) {
				if (fittnessScores[j] > highest_score) {
					highest_score = fittnessScores[j];
					highest_index = j;
				}

				if (fittnessScores[j] < lowest_score) {
					lowest_score = fittnessScores[j];
					lowest_index = j;
				}
			}

			if (smallLessHealthy) {
				System.out.println("Generation: " + i
						+ "; Best fittness score: " + highest_score);
				System.out.println("soilType "
						+ soilTypes[(int) gene[highest_index][0]]
						+ "; localToHybrid maize ratio "
						+ gene[highest_index][1] + "; planting days Std "
						+ gene[highest_index][2] + "; random number seed "
						+ gene[highest_index][3]);
			} else {
				System.out.println("Generation: " + i
						+ "; Best fitness score: " + lowest_score);
				System.out.println("soilType "
						+ soilTypes[(int) gene[lowest_index][0]]
						+ "; localToHybrid maize ratio "
						+ gene[lowest_index][1] + "; planting days Std "
						+ gene[lowest_index][2] + "; random number seed "
						+ gene[lowest_index][3]);
			}

			// Deme size of half the population
			// int demeSize = popSize / 2;
			microbial_tournament(demeSize, 0.5, 0.5, gene, fittnessScores,
					smallLessHealthy);
		}

		double highest_score = 0;
		double lowest_score = Double.MAX_VALUE;
		int highest_index = 0, lowest_index = 0;

		System.out
				.println("score\tsoilType\tlocalToHybrid maize ratio\tplanting days Std\trandom number seed");
		for (int j = 0; j < fittnessScores.length; j++) {
			if (fittnessScores[j] > highest_score) {
				highest_score = fittnessScores[j];
				highest_index = j;
			}

			if (fittnessScores[j] < lowest_score) {
				lowest_score = fittnessScores[j];
				lowest_index = j;
			}

			System.out.println(fittnessScores[j] + "\t"
					+ soilTypes[(int) gene[j][0]] + "\t" + gene[j][1] + "\t"
					+ gene[j][2] + "\t" + gene[j][3]);
		}

		if (smallLessHealthy) {
			System.out.println("Best fitness score: " + highest_score);
			System.out.println("soilType "
					+ soilTypes[(int) gene[highest_index][0]]
					+ "; localToHybrid maize ratio " + gene[highest_index][1]
					+ "; planting days Std " + gene[highest_index][2]
					+ "; random number seed " + gene[highest_index][3]);
		} else {
			System.out.println("Best fitness score: " + lowest_score);
			System.out.println("soilType "
					+ soilTypes[(int) gene[lowest_index][0]]
					+ "; localToHybrid maize ratio " + gene[lowest_index][1]
					+ "; planting days Std " + gene[lowest_index][2]
					+ "; random number seed " + gene[lowest_index][3]);
		}

		// Remember the finish time
		long finishTime = new Date().getTime();
		System.out
				.println("Finished in: "
						+ String.format(
								"%d min, %d sec",
								TimeUnit.MILLISECONDS.toMinutes(finishTime
										- startTime),
								TimeUnit.MILLISECONDS.toSeconds(finishTime
										- startTime)
										- TimeUnit.MINUTES
												.toSeconds(TimeUnit.MILLISECONDS
														.toMinutes(finishTime
																- startTime))));
	}
}
