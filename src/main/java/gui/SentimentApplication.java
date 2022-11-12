package gui;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class SentimentApplication extends Application {

    public static void main(String[] args){
        launch(args);
    }

    @Override
    public void start(Stage stage) throws Exception {

        stage.setTitle("Twitter Sentiment Analysis");
        stage.setMinHeight(400);
        stage.setMinWidth(400);

        FXMLLoader fxmlLoader = new FXMLLoader(getClass().getResource("/GuiScenes/WelcomeScene.fxml"));
        Scene scene = new Scene(fxmlLoader.load());

        stage.setScene(scene);
        stage.show();
    }


}
