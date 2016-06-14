package algorithms;

import java.util.Random;
public class SimpleMicrobialAlgorithm {
	// static int MAX_DOLLAR_AMT = 10;
	static double TARGET_CENT_AMT = 5678;
	// static long seed = 24;
	static Random rd = new Random();

	/*
	 * @param: D, deme size
	 * 
	 * @param: REC, recombination rate
	 * 
	 * @param: MUT, mutation rate
	 */
	public static void microbial_tournament(int D, double REC, double MUT,
			double[][] gene) {
		int P = gene.length;
		int N = gene[0].length;

		int A = (int) (P * rd.nextDouble());
		int B = (A + 1 + (int) (D * rd.nextDouble())) % P;

		int W, L;
		if (eval(A, gene) > eval(B, gene)) {
			W = A;
			L = B;
		} else {
			W = B;
			L = A;
		}

		for (int i = 0; i < N; i++) {
			if (rd.nextDouble() < REC) {
				gene[L][i] = gene[W][i];
			}

			if (rd.nextDouble() < MUT) {
				gene[L][i] += rd.nextGaussian();

				// if (gene[L][i] < MIN) {
				// gene[L][i] = MIN;
				// } else if (gene[L][i] > MAX) {
				// gene[L][i] = MAX;
				// }
			}
		}
	}

	public static void int_microbial_tournament(int D, double REC, double MUT,
			int[][] gene) {
		int P = gene.length;
		int N = gene[0].length;

		int A = rd.nextInt(P);
		int B = (A + 1 + (int) (D * rd.nextDouble())) % P;

		int W, L;
		if (int_eval(A, gene) > int_eval(B, gene)) {
			W = A;
			L = B;
		} else {
			W = B;
			L = A;
		}

		for (int i = 0; i < N; i++) {
			if (rd.nextDouble() < REC) {
				gene[L][i] = gene[W][i];
			}

			if (rd.nextDouble() < MUT) {
				gene[L][i] = (int) (gene[L][i] + rd.nextGaussian() * 10);
				
				if (gene[L][i] < 0) {
					gene[L][i] = 0;
				} else if (gene[L][i] > 9) {
					gene[L][i] = 9;
				}
			}
		}
	}

	/*
	 * fitness evaluation function
	 */
	public static double eval(int A, double[][] gene) {
		return 0;
	}

	public static double int_eval(int A, int[][] gene) {
		// double score = 0;
		// for (int i = 0; i < gene[A].length; i++) {
		// score += gene[A][i] * 1.0 / MAX_DOLLAR_AMT;
		// }

		double sum = 0;
		// sum += gene[A][0]; // cent
		// sum += gene[A][1] * 10; // penny
		// sum += gene[A][2] * 25; // quarter
		// sum += gene[A][3] * 100; // dollar

		sum += gene[A][0];
		sum += gene[A][1] * 10;
		sum += gene[A][2] * 100;
		sum += gene[A][3] * 1000;

		return 1 - Math.abs(TARGET_CENT_AMT - sum) / 10000.0;
	}

	public static void main(String[] args) {
		int[][] gene = new int[10][4];
		// initialization
		for (int i = 0; i < gene.length; i++) {
			for (int j = 0; j < gene[0].length; j++) {
				gene[i][j] = rd.nextInt(10);
			}
		}

		int MAX_GENERATION_NUM = 100;
		for (int i = 0; i < MAX_GENERATION_NUM; i++) {
			double highest_score = 0;
			int fittest_index = 0;
			for (int j = 0; j < gene.length; j++) {
				double current_score = int_eval(j, gene);
				if (current_score > highest_score) {
					highest_score = current_score;
					fittest_index = j;
				}
			}

			System.out.println("Generation: " + i + "; Best fittness score: "
					+ highest_score);
			// System.out.println(gene[fittest_index][3] + " dollor "
			// + gene[fittest_index][2] + " quarter "
			// + gene[fittest_index][1] + " dime "
			// + gene[fittest_index][0] + " cent");
			System.out.println(gene[fittest_index][3] + " "
					+ gene[fittest_index][2] + " " + gene[fittest_index][1]
					+ " " + gene[fittest_index][0]);
			int_microbial_tournament(1, 0.5, 0.5, gene);
		}
	}
}
