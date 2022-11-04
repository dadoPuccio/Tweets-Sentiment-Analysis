package classifier;

import java.io.*;

import com.aliasi.classify.Classification;
import com.aliasi.classify.Classified;
import com.aliasi.classify.DynamicLMClassifier;
import com.aliasi.classify.LMClassifier;
import com.aliasi.lm.LanguageModel;
import com.aliasi.lm.NGramProcessLM;
import com.aliasi.stats.MultivariateDistribution;
import com.aliasi.util.CommaSeparatedValues;

public class SentimentClassifier {

    private static final String TRAIN_FILE_NAME = "./src/main/resources/train.csv";
    private static final String TEST_FILE_NAME = "./src/main/resources/test.csv";
    private static final String MODEL_FILE_NAME = "./src/main/resources/SentimentClassifier.model";

    private static final String[] CATEGORIES = {"0", "1"};
    private static final int NGRAM = 5;

    private DynamicLMClassifier<NGramProcessLM> sentimentClassifier;
    private LMClassifier<LanguageModel, MultivariateDistribution> trainedSentimentClassifier;

    public SentimentClassifier() {
        sentimentClassifier = DynamicLMClassifier.createNGramProcess(CATEGORIES, NGRAM);
    }

    public SentimentClassifier(File model) throws IOException, ClassNotFoundException {
        FileInputStream fileIn = new FileInputStream(model);
        ObjectInputStream objIn = new ObjectInputStream(fileIn);
        trainedSentimentClassifier = (LMClassifier) objIn.readObject();
        objIn.close();
    }

    public String classify(String text) {
        return trainedSentimentClassifier.classify(text).bestCategory();
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        File model = new File(MODEL_FILE_NAME);

        if(!model.exists()){
            SentimentClassifier classifier = new SentimentClassifier();
            classifier.train();
            classifier.save();
        } else {
            SentimentClassifier classifier = new SentimentClassifier(model);
            classifier.evaluate();
        }
    }

    private void train() throws IOException {

        File file = new File(TRAIN_FILE_NAME);
        CommaSeparatedValues csv = new CommaSeparatedValues(file, "UTF-8");
        String[][] rows = csv.getArray();

        String sentiment, text;
        Classification classification;
        Classified<CharSequence> classified;

        for(String[] row : rows){
            if(row.length == 7) {
                sentiment = row[0];
                if (sentiment.equals("4"))
                    sentiment = "1";
                classification = new Classification(sentiment);

                text = row[5];

                classified = new Classified<>(text, classification);
                sentimentClassifier.handle(classified);
            }
        }
    }

    private void evaluate() throws IOException {
        File file = new File(TEST_FILE_NAME);
        CommaSeparatedValues csv = new CommaSeparatedValues(file, "UTF-8");
        String[][] rows = csv.getArray();

        int numTests = 0;
        int numCorrect = 0;

        String groundTruth, text;
        Classification classification;

        for(String[] row : rows){
            if(row.length == 7) {
                numTests += 1;
                groundTruth = row[0];
                if (groundTruth.equals("4"))
                    groundTruth = "1";

                text = row[5];

                classification = trainedSentimentClassifier.classify(text);

                if (classification.bestCategory().equals(groundTruth))
                    numCorrect++;
            }
        }

        System.out.println("# Test Instances = " + numTests);
        System.out.println("# Correct = " + numCorrect);
        System.out.println("% Correct = " + ((double)numCorrect)/(double)numTests);
    }

    private void save() throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(MODEL_FILE_NAME);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
        sentimentClassifier.compileTo(objectOutputStream);
        objectOutputStream.close();
    }

}
