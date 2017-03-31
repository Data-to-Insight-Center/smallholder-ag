package algorithms;
import java.io.IOException;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import scala.reflect.Print;
import controller.GeneticAlgorithmInterface;


public class SimulatedAnnealing {
	static Random rd = new Random();
	
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
	/*
	public static double[][] generateNeighborPoint() {
		double[][] gene = new double [1][4];
		for (int i = 0; i < 4; i++) {
				if (i == 0) {
					gene[0][i] = rd.nextInt(GeneticAlgorithmInterface
							.getSoilTypes().length)
							% GeneticAlgorithmInterface.getSoilTypes().length;
				} else if (i == 1) {
					gene[0][i] += rd.nextGaussian();

					if (gene[0][i] < 0) {
						gene[0][i] = 0;
					} else if (gene[0][i] > 1) {
						gene[0][i] = 1;
					}
				} else if (i == 3) {
					gene[0][i] = rd.nextInt();
				} else {
					gene[0][i] += rd.nextGaussian() * 0.167;

					if (gene[0][i] <= 0) {
						gene[0][i] = 0.01;
					} else if (gene[0][i] > 0.167) {
						gene[0][i] = 0.167;
					}
				}
		}
		return gene;
	}
	*/

	 public static double[][] generateNeighborPoint(double[][] prevGene) {
		double[][] gene = new double[1][4];
		
		//random change on gene 0
		if (rd.nextBoolean()) {
			gene[0][0] = rd.nextInt(GeneticAlgorithmInterface
					.getSoilTypes().length)
					% GeneticAlgorithmInterface.getSoilTypes().length;
		} else {
			gene[0][0] = prevGene[0][0];
		}
		
		//random change on gene 1
		if (rd.nextBoolean()) {
			gene[0][1] += rd.nextGaussian();
			if (gene[0][1] < 0) {
				gene[0][1] = 0;
			} else if (gene[0][1] > 1) {
				gene[0][1] = 1;
			}
		} else {
			gene[0][1] = prevGene[0][1];
		}
		
		//random change on gene 2
		if(rd.nextBoolean()){
			gene[0][2] += rd.nextGaussian() * 0.167;

			if (gene[0][2] <= 0) {
				gene[0][2] = 0.01;
			} else if (gene[0][2] > 0.167) {
				gene[0][2] = 0.167;
			}
		} else{
			gene[0][2] = prevGene[0][2];
		}
		
		//random change on gene 3
		if (rd.nextBoolean()) {
			gene[0][3] = rd.nextInt();
		} else {
			gene[0][3] = prevGene[0][3];
		}
		
		
		return gene;
	}
	

	public static void main(String[] args) {
		long startTime = new Date().getTime();
	
		String[] soilTypes = GeneticAlgorithmInterface.getSoilTypes();
		double [][] startPoint = new double [1][4];
		
		//initialization
		int numOfRun =1;
		double temp = 1000;
		double coolRate = 0.9;
		
		startPoint[0][0] = rd.nextInt(soilTypes.length); // soilType
		startPoint[0][1] = rd.nextDouble(); // local to hybrid maize ratio
		startPoint[0][2] = 0.001 + rd.nextDouble() * (0.167 - 0.001); // std dev 0.001 - 0.167
		startPoint[0][3] = rd.nextInt(); // random seeds
		//get current fitness score
		//double fittnessScoreOfCurrentPoint = eval(0, startPoint);
		double fittnessScoreOfCurrentPoint = 0.0;
		//keep running until temperature lower down to 1	
		while(temp > 1) {
			System.out.println("Current Run:"+numOfRun+"");
			//generate a new neighbor point 
			double[][] newPoint = generateNeighborPoint(startPoint);
			if (startPoint == newPoint) {
				temp *= coolRate;
				numOfRun +=1;
				continue;
			}
			//get new fitness score
			//double fittnessScoreOfNewPoint = eval(0, newPoint);
			double fittnessScoreOfNewPoint = 1.0;
			if(fittnessScoreOfNewPoint > fittnessScoreOfCurrentPoint) {
				startPoint = newPoint;
				fittnessScoreOfCurrentPoint = fittnessScoreOfNewPoint;
				System.out.println("new parameter setting with higher score:"+fittnessScoreOfNewPoint+"");
			//if this fit random function that allowed to accept lower value	
			} else if (Math.exp((fittnessScoreOfCurrentPoint - fittnessScoreOfNewPoint)/temp) > rd.nextDouble()) {
				startPoint = newPoint;
				fittnessScoreOfCurrentPoint = fittnessScoreOfNewPoint;
				System.out.println("new parameter setting with lower score:"+fittnessScoreOfNewPoint+"");
			} else {
				startPoint = startPoint;
				fittnessScoreOfCurrentPoint = fittnessScoreOfCurrentPoint;
				System.out.println("keep current parameter:"+fittnessScoreOfCurrentPoint+"");
			}
			
			temp *= coolRate;
			numOfRun +=1;
		}
		
		System.out.println("final parameter find as");
		System.out.println(fittnessScoreOfCurrentPoint);
		System.out.println(startPoint[0][0]+" "+startPoint[0][1]+" "+startPoint[0][2]+" "+startPoint[0][3]);
		
		//System wall time 
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
 