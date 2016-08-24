package ru.hokan.text;

import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.UUID;

public class IpDataCountReducer extends Reducer<Text, Text, DocumentURI, MarkLogicNode> {


    private static final Log LOGGER = LogFactory.getLog(IpDataCountReducer.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void reduce(Text keyIn, Iterable<Text> valuesIn, Context context)
            throws IOException, InterruptedException {

        Integer totalNumberOfBytes = 0;
        int numberOfValues = 0;
        for (Text text : valuesIn) {
            String value = text.toString();
            String[] split = value.split("\\s");
            float averageBytesCount = Float.parseFloat(split[0]);
            int totalBytesCount = Integer.parseInt(split[1]);
            totalNumberOfBytes += totalBytesCount;
            if (averageBytesCount != 0) {
                numberOfValues += Math.ceil(totalBytesCount / averageBytesCount);
            } else {
                numberOfValues++;
            }
        }

        Document document = createDocument(keyIn, totalNumberOfBytes, numberOfValues);
        if (document == null) {
            return;
        }

        MarkLogicNode node = new MarkLogicNode(document);
        DocumentURI uri = new DocumentURI();
        uri.setUri(UUID.randomUUID().toString());
        context.write(uri, node);
    }

    private Document createDocument(Text keyIn, Integer totalNumberOfBytes, int numberOfValues) {
        DocumentBuilder docBuilder;
        try {
            docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        } catch (ParserConfigurationException e) {
            LOGGER.error(e.getMessage(), e);
            return null;
        }

        Document document = docBuilder.newDocument();
        Element rootElement = document.createElement("data");
        document.appendChild(rootElement);
        Element ipDataElement = document.createElement("ip-data");
        rootElement.appendChild(ipDataElement);

        Element ipNameElement = document.createElement("ip-name");
        ipNameElement.setTextContent(keyIn.toString());
        ipDataElement.appendChild(ipNameElement);

        Element averageBytesValueElement = document.createElement("avg-value");
        float averageNumberOfBytes = (float) totalNumberOfBytes / numberOfValues;
        averageBytesValueElement.setTextContent(String.valueOf(averageNumberOfBytes));
        ipDataElement.appendChild(averageBytesValueElement);

        Element totalBytesValueElement = document.createElement("total-value");
        totalBytesValueElement.setTextContent(String.valueOf(totalNumberOfBytes));
        ipDataElement.appendChild(totalBytesValueElement);
        return document;
    }
}