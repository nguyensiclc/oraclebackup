package com.oracleexporter.gui;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Scene;
import javafx.stage.Stage;

import java.io.IOException;

public class OracleSchemaExporterApp extends Application {

    @Override
    public void start(Stage stage) throws IOException {
        FXMLLoader loader = new FXMLLoader(OracleSchemaExporterApp.class.getResource("/fxml/main-view.fxml"));
        Scene scene = new Scene(loader.load(), 1100, 720);
        scene.getStylesheets().add(OracleSchemaExporterApp.class.getResource("/css/app.css").toExternalForm());
        stage.setTitle("OracleSchemaExporter");
        stage.setScene(scene);
        MainController controller = loader.getController();
        stage.setOnCloseRequest(e -> controller.shutdown());
        stage.show();
    }

    public static void main(String[] args) {
        launch(args);
    }
}

