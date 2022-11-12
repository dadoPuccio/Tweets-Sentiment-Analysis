package gui;

import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import javafx.scene.input.MouseEvent;
import javafx.scene.paint.Color;
import javafx.stage.Stage;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.ResourceBundle;

public class WelcomeSceneController implements Initializable {

    @FXML
    private TextField k1, k2, k3, k4, k5;
    @FXML
    private Label feedback;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {

    }


    public void submitEvent(MouseEvent mouseEvent) throws IOException {
        if(k1.getText().equals("") && k2.getText().equals("") &&
                k3.getText().equals("") && k4.getText().equals("") &&
                k5.getText().equals("")){
            feedback.setText("No keys are set!");
            feedback.setTextFill(Color.RED);
        } else {
            feedback.setText("");
            feedback.setTextFill(Color.BLACK);

            Stage currentStage = (Stage) ((Node)mouseEvent.getSource()).getScene().getWindow(); // get current stage

            FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/GuiScenes/MainScene.fxml"));
            Scene scene = new Scene(fxmlLoader.load()); // load main scene

            currentStage.setScene(scene);

            MainSceneController controller = fxmlLoader.getController();

            ArrayList<String> keywordsList = new ArrayList<>();
            if(!k1.getText().equals("")) keywordsList.add(k1.getText());
            if(!k2.getText().equals("")) keywordsList.add(k2.getText());
            if(!k3.getText().equals("")) keywordsList.add(k3.getText());
            if(!k4.getText().equals("")) keywordsList.add(k4.getText());
            if(!k5.getText().equals("")) keywordsList.add(k5.getText());

            String[] keywords = new String[keywordsList.size()];
            for(int i = 0; i < keywords.length; i ++)
                keywords[i] = keywordsList.get(i);
            controller.startAnalysis(keywords);

        }
    }
}
