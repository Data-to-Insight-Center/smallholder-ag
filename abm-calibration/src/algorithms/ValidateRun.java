package algorithms;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import controller.GeneticAlgorithmInterface;

/**
 * soilIndex is based on the list in GeneticAlgorithmInterface, pasted below:
 * private static String[] soilTypes = { "WI_FLBD007", "WI_FRAO009",
 * "WI_ACLS021", "WI_CMZR003", "WI_VRZM080", "WI_GLBW752", "WI_VRBW446",
 * "WI_CMTR038", "WI_ARBW401", "WI_LVUY032", "WI_CMTN008", "WI_FLSO001",
 * "WI_LVLS007", "WI_PHCF014", "WI_CMYE107" }
 * 
 * */
public class ValidateRun {
	public static void main(String[] args) {
		if (args.length != 6) {
			System.out
					.println("Input parameters: localToHybridRatio soilIndex pDayStd year randomSeed targetYield");
		}

		double localToHybridRatio = Double.parseDouble(args[0]);
		int soilIndex = Integer.parseInt(args[1]);
		double pDayStd = Double.parseDouble(args[2]);
		int year = Integer.parseInt(args[3]);
		int randomSeed = Integer.parseInt(args[4]);
		double targetYield = Double.parseDouble(args[5]);

		long startTime = new Date().getTime();

		try {
			GeneticAlgorithmInterface.initializeModel(localToHybridRatio,
					soilIndex, pDayStd, year, randomSeed);
			GeneticAlgorithmInterface.runSim();
			// double avgYield = GeneticAlgorithmInterface.getAvgYield(year,
			// GeneticAlgorithmInterface.getSoilTypes()[soilIndex]);
			// double fittnessScore = 1 - Math.abs(targetYield - avgYield)
			// / targetYield;
			//
			// System.out.println("simulatedAvgYield: " + avgYield
			// + "\nfittnessScore: " + fittnessScore);
			double[] breaks = { 0, 1000, 2000, 3000, 4000, 5000, 6000, 7000,
					8000, 9000 };
			int[] counts = { 1015, 714, 496, 166, 134, 63, 40, 16, 26, 4 };
			int totalCount = 0;
			for (int n : counts) {
				totalCount += n;
			}

			double[] dist = new double[counts.length];
			for (int i = 0; i < counts.length; i++) {
				dist[i] = counts[i] * 1.0 / totalCount;
			}

			double klDiv = GeneticAlgorithmInterface.getKLDivergence(year,
					GeneticAlgorithmInterface.getSoilTypes()[soilIndex], dist,
					breaks);
			System.out.println("Best fitness score: " + klDiv);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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
