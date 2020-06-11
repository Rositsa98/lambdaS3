package com.amazonaws.lambda.demo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private static final String RESIZED_SUFFIX = "-resized";
	private static final String SENTIMENTED_PREFFIX = "sentimented-";
	private static final String APPEND_PREFIX = "append-";

	private List<String> stopwordsInput = new ArrayList<String>();
	private List<String> reviewsInput = new ArrayList<String>();
	private Map<String, Double> sentimentValues = new HashMap<>();
	private Map<String, Long> frequency = new HashMap<>();

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

	public LambdaFunctionHandler() {
	}

	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event s3uploadReviewEvent, Context context) {
		context.getLogger().log("Received event: " + s3uploadReviewEvent);

		S3EventNotificationRecord eventRecord = s3uploadReviewEvent.getRecords().get(0);

		String sourceBucketName = eventRecord.getS3().getBucket().getName();
		String destinationBucketName = sourceBucketName + RESIZED_SUFFIX;
		AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

		if (checkBucketsDifference(sourceBucketName, destinationBucketName)) {
			String sourceKey = eventRecord.getS3().getObject().getUrlDecodedKey(); // Get the source name by it's key
			String destinatinKey = SENTIMENTED_PREFFIX + sourceKey;

			try (InputStream review = downloadUploadedReviewFile(s3Client, sourceBucketName, sourceKey);
					final Reader reader = new InputStreamReader(review)) {

				String textReview = IOUtils.toString(review);

				if (sourceKey.startsWith(APPEND_PREFIX)) {
					appendReview(textReview, Integer.parseInt(textReview.split(" ")[0]));
				}

				String sentimentedReview = calculateSentiment(textReview);

				InputStream targetStream = new ByteArrayInputStream(sentimentedReview.getBytes());

				uploadSentimentedReviewToTargetBucket(s3Client, targetStream, destinationBucketName, destinatinKey);

				System.out.println("Successfully sentimented " + sourceBucketName + "/" + sourceKey
						+ " and uploaded to " + destinationBucketName + "/" + destinatinKey);
				return "Ok";
			} catch (IOException e) {
				System.out.println("Error reading input review file." + e.getCause());
				throw new RuntimeException(e);
			}
		}
		return "";
	}

	/**
	 * Sanity check: validate that source and destination are different buckets.
	 * 
	 * @param sourceBucketName
	 * @param destinationBucketName
	 * @return true/false
	 */
	private boolean checkBucketsDifference(String sourceBucketName, String destinationBucketName) {
		if (sourceBucketName.equals(destinationBucketName)) {
			System.out.println("Destination bucket must not match source bucket.");
			return false;
		}
		return true;
	}

	/**
	 * Method used to download uploaded review file from source S3 bucket in order
	 * to calculate it's sentiment.
	 * 
	 * @param sourceBucketName
	 * @param sourceKey
	 * @return - InputStream of review's content
	 */
	private InputStream downloadUploadedReviewFile(AmazonS3 s3Client, String sourceBucketName, String sourceKey) {
		System.out
				.println("Download the file: " + sourceKey + " from S3 bucket: " + sourceBucketName + " into a stream");
		S3Object s3Object = s3Client.getObject(new GetObjectRequest(sourceBucketName, sourceKey));
		InputStream review = s3Object.getObjectContent();

		return review;
	}

	private void uploadSentimentedReviewToTargetBucket(AmazonS3 s3Client, InputStream targetStream,
			String destinationBucketName, String destinatinKey) {
		System.out.println("Uploading to S3 destination bucket: " + destinationBucketName + "/" + destinatinKey);
		try {
			s3Client.putObject(destinationBucketName, destinatinKey, targetStream, new ObjectMetadata());
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		}
	}

	private static String topWords = "";

	protected String calculateSentiment(String review) {
		String sentimentedReview = null;

		ClassLoader classLoader = getClass().getClassLoader();
		File inputReviews = new File(classLoader.getResource("reviewsInput.txt").getFile());
		File stopWordsFile = new File(classLoader.getResource("stopWords.txt").getFile());

		try (BufferedReader reader = new BufferedReader(new FileReader(stopWordsFile));
				BufferedReader reader2 = new BufferedReader(new FileReader(inputReviews))) {

			stopwordsInput = reader.lines().collect(Collectors.toList());
			reviewsInput = reader2.lines().collect(Collectors.toList());
			setUpSentimentMap();
			setUpFrequencyMap();
			Set<String> mostFrequentWords = getMostFrequentWords(3);
			mostFrequentWords.stream().forEach(word -> topWords += (word + " "));

			double sentimentValue = (int) Math.round(getReviewSentiment(review));
			String stringSentimentValue = getReviewSentimentAsName(review);
			sentimentedReview = sentimentValue + " (" + stringSentimentValue + ") " + review + "\n\n"
					+ " most frequent words from input reviews are: " + topWords;

		} catch (IOException e) {
			throw new UnsupportedOperationException("calculateSentiment: IOException when reading input files.");
		}

		return sentimentedReview;
	}

	public double getReviewSentiment(String review) {
		String[] words = review.split("[^\\w']+");
		// case 1: Unknown words only
		int idx = 0;
		long countUnknownWords = 0;
		for (String word : words) {
			if (isStopWord(words[idx])) {
				countUnknownWords++;
			}
			idx++;
		}
		if (countUnknownWords == words.length) {
			return -1;
		}
		// case 2: Calculate sentiment
		double sum = 0;
		int countWords = 0;
		for (String word : words) {
			if (sentimentValues.get(word.toLowerCase()) != null) {
				sum += sentimentValues.get(word.toLowerCase());
				countWords++;
			}
		}
		if (countWords > 0) {
			return sum / countWords;
		} else {
			return -1;
		}
	}

	public boolean isStopWord(String word) {
		return containsCaseInsensitiveInList(word, stopwordsInput) || word.equals("") || word.equals(" ")
				|| !word.matches("[a-zA-Z0-9]*") || !containsCaseInsensitiveInList(word, reviewsInput);
	}

	private boolean containsCaseInsensitiveInList(String string, List<String> lines) {
		for (String line : lines) {
			if (containsCaseInsensitive(string, line)) {
				return true;
			}
		}
		return false;
	}

	private boolean containsCaseInsensitive(String string, String line) {
		String[] words = line.split("[^\\w']+");

		for (String word : words) {
			if (word.equalsIgnoreCase(string)) {
				return true;
			}
		}
		return false;
	}

	void setUpSentimentMap() {
		for (String line : reviewsInput) {
			String[] words = line.split("[^\\w']+");
			int idx = 0;

			while (idx < words.length) {
				if (!isStopWord(words[idx])) {
					sentimentValues.put(words[idx].toLowerCase(), getWordSentiment(words[idx]));
				}
				idx++;
			}
		}
	}

	private static double sum = 0;

	public double getWordSentiment(String word) {
		sum = 0;
		if (isStopWord(word)) {
			return -1;
		} else {
			List<String> containedRows = reviewsInput.stream().filter(l -> containsCaseInsensitive(word, l))
					.collect(Collectors.toList());

			containedRows.stream().forEach(l -> {
				sum += Integer.parseInt(l.substring(0, 1));
			});

			return sum / containedRows.size();
		}
	}

	public String getReviewSentimentAsName(String review) {
		int score = (int) Math.round(getReviewSentiment(review));

		switch (score) {
		case 0: {
			return "negative";
		}
		case 1: {
			return "somewhat negative";
		}
		case 2: {
			return "neutral";
		}
		case 3: {
			return "somewhat positive";
		}
		case 4: {
			return "positive";
		}
		}
		return "unknown";
	}

	public void appendReview(String review, int sentimentValue) {
		ClassLoader classLoader = getClass().getClassLoader();
		File inputReviews = new File(classLoader.getResource("reviewsInput.txt").getFile());

		StringBuilder sb = new StringBuilder();
		sb.append("");
		sb.append(sentimentValue);

		String newReview = System.lineSeparator() + sb.toString() + " " + review;

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(inputReviews, true));) {

			writer.append(newReview);

		} catch (IOException e) {
			System.out.println("IOException found in appending review.");
		}

		this.reviewsInput = new ArrayList<String>();

		try (BufferedReader br2 = new BufferedReader(new FileReader(inputReviews))) {

			this.reviewsInput = br2.lines().collect(Collectors.toList());

		} catch (IOException e) {
			throw new UnsupportedOperationException("Method not yet implemented");
		}

		frequency = new HashMap<>();
		sentimentValues = new HashMap<>();

		setUpFrequencyMap();
		setUpSentimentMap();

	}

	void setUpFrequencyMap() {

		for (String line : reviewsInput) {
			String[] words = line.split("[^\\w']+");

			int idx = 0;

			while (idx < words.length) {
				if (!isStopWord(words[idx])) {

					long count = getCountOfWordMeetingsInReviewsInput(words[idx]);
					frequency.put(words[idx].toLowerCase(), count);
				}
				idx++;
			}
		}
	}

	private static long count = 0;

	private long getCountOfWordMeetingsInReviewsInput(String word) {
		count = 0;
		reviewsInput.stream().forEach(l -> {
			if (containsCaseInsensitive(word, l)) {
				count += countCaseInsensitiveWordInRow(word, l);
			}
		});
		return count;
	}

	private long countCaseInsensitiveWordInRow(String string, String line) {
		int countInRow = 0;
		String[] words = line.split("[^\\w']+");

		for (String word : words) {
			if (word.equalsIgnoreCase(string)) {
				countInRow++;
			}
		}
		return countInRow;
	}

	public Set<String> getMostFrequentWords(int n) {

		sortFrequencyMapReversed();

		Set<String> frequentWords = new HashSet<>();

		List<Entry<String, Long>> list = new ArrayList<>(frequency.entrySet());

		if (n < 0) {
			try {
				throw new IllegalArgumentException("Size<0");
			} catch (IllegalArgumentException e) {
				System.out.println("IllegalArgumentException found in getting most frequent words.");
			}

		} else {
			for (int i = 0; i < n; i++) {
				if (i < list.size()) {
					frequentWords.add(list.get(i).getKey());
				}
			}
		}
		return frequentWords;
	}

	private void sortFrequencyMapReversed() {
		frequency = sortByValueReversed(frequency);
	}

	public <K, V extends Comparable<? super V>> Map<K, V> sortByValueReversed(Map<K, V> map) {
		List<Entry<K, V>> list = new ArrayList<>(map.entrySet());
		list.sort(Entry.comparingByValue(Collections.reverseOrder()));

		Map<K, V> result = new LinkedHashMap<>();
		for (Entry<K, V> entry : list) {
			result.put(entry.getKey(), entry.getValue());
		}

		return result;
	}

}