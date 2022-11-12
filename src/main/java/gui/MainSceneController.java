package gui;

import javafx.beans.property.SimpleStringProperty;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.chart.BarChart;
import javafx.scene.chart.XYChart;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.MouseEvent;
import servingLayer.ServingLayer;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.ResourceBundle;

public class MainSceneController implements Initializable {

    @FXML private Button stopButton;

    @FXML private TableView<Row> batchTable;
    @FXML private TableColumn<Row, String> batchKeywords;
    @FXML private TableColumn<Row, String> batchPositive;
    @FXML private TableColumn<Row, String> batchNegative;

    @FXML private TableView<Row> realTimeTable;
    @FXML private TableColumn<Row, String> realTimeKeywords;
    @FXML private TableColumn<Row, String> realTimePositive;
    @FXML private TableColumn<Row, String> realTimeNegative;

    @FXML private BarChart<String, Integer> combinedChart;
    private XYChart.Series<String, Integer> combinedPositive;
    private XYChart.Series<String, Integer> combinedNegative;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

        batchKeywords.setCellValueFactory(new PropertyValueFactory<>("keyword"));
        batchPositive.setCellValueFactory(new PropertyValueFactory<>("positive"));
        batchNegative.setCellValueFactory(new PropertyValueFactory<>("negative"));

        realTimeKeywords.setCellValueFactory(new PropertyValueFactory<>("keyword"));
        realTimePositive.setCellValueFactory(new PropertyValueFactory<>("positive"));
        realTimeNegative.setCellValueFactory(new PropertyValueFactory<>("negative"));

        combinedPositive = new XYChart.Series<>();
        combinedPositive.setName("positive");

        combinedNegative = new XYChart.Series<>();
        combinedNegative.setName("negative");
    }

    public void startAnalysis(String[] keywords) {

        // starting the architecture asynchronously
        Thread LambdaArchitectureThread = new Thread(new ServingLayer(keywords, this));
        LambdaArchitectureThread.start();

        for(String keyword : keywords) {
            batchTable.getItems().add(new Row(keyword, "0", "0"));
            realTimeTable.getItems().add(new Row(keyword, "0", "0"));

            combinedPositive.getData().add(new XYChart.Data<>(keyword, 0));
            combinedNegative.getData().add(new XYChart.Data<>(keyword, 0));
        }

        combinedChart.getData().add(combinedPositive);
        combinedChart.getData().add(combinedNegative);
        updateCombinedChart();

    }

    public void stopAnalysis(MouseEvent mouseEvent) {
        ServingLayer.stop();
        stopButton.setText("Stopping the architecture...");
        stopButton.setMinWidth(260);
        stopButton.setDisable(true);
    }

    public void updateRealTimeView() throws IOException {
        HashMap<String, HashMap<String, Integer>> counts = ServingLayer.getSpeedLayerCounts();
        int nPositive;
        int nNegative;

        for(Row row : realTimeTable.getItems()){
            if(counts.containsKey(row.getKeyword())) {
                nPositive = counts.get(row.getKeyword()).get("positive");
                nNegative = counts.get(row.getKeyword()).get("negative");

                row.setPositive(Integer.toString(nPositive));
                row.setNegative(Integer.toString(nNegative));
            } else {
                row.setPositive("0");
                row.setNegative("0");
            }
        }
        realTimeTable.refresh();
        updateCombinedChart();
    }

    public void updateBatchView() throws IOException {
        HashMap<String, HashMap<String, Integer>> counts = ServingLayer.getBatchLayerCounts();
        int nPositive;
        int nNegative;

        // System.out.println(counts.keySet());

        for(Row row : batchTable.getItems()){
            if(counts.containsKey(row.getKeyword())) {
                nPositive = counts.get(row.getKeyword()).get("positive");
                nNegative = counts.get(row.getKeyword()).get("negative");

                row.setPositive(Integer.toString(nPositive));
                row.setNegative(Integer.toString(nNegative));
            }
        }

        batchTable.refresh();
        updateCombinedChart();
    }

    private void updateCombinedChart() {

        for(XYChart.Data<String, Integer> el : combinedPositive.getData()){
            int batchPositive = Integer.parseInt(batchTable.getItems().filtered(row -> row.getKeyword().equals(el.getXValue())).get(0).getPositive());
            int realTimePositive = Integer.parseInt(realTimeTable.getItems().filtered(row -> row.getKeyword().equals(el.getXValue())).get(0).getPositive());
            el.setYValue(batchPositive + realTimePositive);
        }

        for(XYChart.Data<String, Integer> el : combinedNegative.getData()){
            int batchNegative = Integer.parseInt(batchTable.getItems().filtered(row -> row.getKeyword().equals(el.getXValue())).get(0).getNegative());
            int realTimeNegative = Integer.parseInt(realTimeTable.getItems().filtered(row -> row.getKeyword().equals(el.getXValue())).get(0).getNegative());
            el.setYValue(batchNegative + realTimeNegative);
        }
    }

    public static class Row {
        private final SimpleStringProperty keyword, positive, negative;

        public Row(String keyword, String positive, String negative){
            this.keyword = new SimpleStringProperty(keyword);
            this.positive = new SimpleStringProperty(positive);
            this.negative = new SimpleStringProperty(negative);
        }

        public String getKeyword() {
            return keyword.get();
        }

        public String getPositive() {
            return positive.get();
        }

        public void setPositive(String batchPositive) {
            this.positive.set(batchPositive);
        }

        public String getNegative() {
            return negative.get();
        }

        public void setNegative(String batchNegative) {
            this.negative.set(batchNegative);
        }
    }
}
