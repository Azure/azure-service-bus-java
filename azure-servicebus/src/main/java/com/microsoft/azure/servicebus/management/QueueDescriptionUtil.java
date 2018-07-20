package com.microsoft.azure.servicebus.management;

import com.microsoft.azure.servicebus.primitives.MessagingEntityNotFoundException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.StringWriter;
import java.time.Duration;

public class QueueDescriptionUtil {

    public static String serialize(QueueDescription queueDescription) throws ParserConfigurationException, TransformerException {
        // todo: Reuse factory
        DocumentBuilderFactory dbFactory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.newDocument();

        Element rootElement = doc.createElementNS(ManagementClientConstants.ATOM_NS, "entry");
        doc.appendChild(rootElement);

        Element contentElement = doc.createElementNS(ManagementClientConstants.ATOM_NS, "content");
        rootElement.appendChild(contentElement);
        contentElement.setAttribute("type", "application/xml");

        Element qdElement = doc.createElementNS(ManagementClientConstants.SB_NS, "QueueDescription");
        contentElement.appendChild(qdElement);

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "LockDuration")
                        .appendChild(doc.createTextNode(queueDescription.lockDuration.toString())).getParentNode());

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "MaxSizeInMegabytes")
                .appendChild(doc.createTextNode(Long.toString(queueDescription.maxSizeInMB))).getParentNode());

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "RequiresDuplicateDetection")
                        .appendChild(doc.createTextNode(Boolean.toString(queueDescription.requiresDuplicateDetection))).getParentNode());

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "RequiresSession")
                        .appendChild(doc.createTextNode(Boolean.toString(queueDescription.requiresSession))).getParentNode());

        if (queueDescription.defaultMessageTimeToLive.compareTo(ManagementClientConstants.MAX_DURATION) < 0) {
            qdElement.appendChild(
                    doc.createElementNS(ManagementClientConstants.SB_NS, "DefaultMessageTimeToLive")
                            .appendChild(doc.createTextNode(queueDescription.defaultMessageTimeToLive.toString())).getParentNode());
        }

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "DeadLetteringOnMessageExpiration")
                        .appendChild(doc.createTextNode(Boolean.toString(queueDescription.enableDeadLetteringOnMessageExpiration))).getParentNode());

        if (queueDescription.requiresDuplicateDetection && queueDescription.duplicationDetectionHistoryTimeWindow.compareTo(Duration.ZERO) > 0) {
            qdElement.appendChild(
                    doc.createElementNS(ManagementClientConstants.SB_NS, "DuplicateDetectionHistoryTimeWindow")
                            .appendChild(doc.createTextNode(queueDescription.duplicationDetectionHistoryTimeWindow.toString())).getParentNode());
        }

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "MaxDeliveryCount")
                        .appendChild(doc.createTextNode(Integer.toString(queueDescription.maxDeliveryCount))).getParentNode());

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "EnableBatchedOperations")
                        .appendChild(doc.createTextNode(Boolean.toString(queueDescription.enableBatchedOperations))).getParentNode());

        // todo: append auth rules
        // todo: append status

        if (queueDescription.forwardTo != null) {
            qdElement.appendChild(
                    doc.createElementNS(ManagementClientConstants.SB_NS, "ForwardTo")
                            .appendChild(doc.createTextNode(queueDescription.forwardTo)).getParentNode());
        }

        if (queueDescription.userMetadata != null) {
            qdElement.appendChild(
                    doc.createElementNS(ManagementClientConstants.SB_NS, "UserMetadata")
                            .appendChild(doc.createTextNode(queueDescription.userMetadata)).getParentNode());
        }

        if (queueDescription.autoDeleteOnIdle.compareTo(ManagementClientConstants.MAX_DURATION) < 0) {
            qdElement.appendChild(
                    doc.createElementNS(ManagementClientConstants.SB_NS, "AutoDeleteOnIdle")
                            .appendChild(doc.createTextNode(queueDescription.autoDeleteOnIdle.toString())).getParentNode());
        }

        qdElement.appendChild(
                doc.createElementNS(ManagementClientConstants.SB_NS, "EnablePartitioning")
                        .appendChild(doc.createTextNode(Boolean.toString(queueDescription.enablePartitioning))).getParentNode());

        if (queueDescription.forwardDeadLetteredMessagesTo != null) {
            qdElement.appendChild(
                    doc.createElementNS(ManagementClientConstants.SB_NS, "ForwardDeadLetteredMessagesTo")
                            .appendChild(doc.createTextNode(queueDescription.forwardDeadLetteredMessagesTo)).getParentNode());
        }

        // Convert dom document to string.
        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer = transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter output = new StringWriter();
        transformer.transform(new DOMSource(doc), new StreamResult(output));
        return output.toString();
    }

    static QueueDescription parseFromContent(String xml) throws MessagingEntityNotFoundException {
        // TODO: Reuse dbf
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document dom = db.parse(new ByteArrayInputStream(xml.getBytes("utf-8")));
            Element doc = dom.getDocumentElement();
            doc.normalize();
            if (doc.getTagName() == "entry")
            return parseFromEntry(doc);
        }
        catch (Exception ex) {
            System.out.println(ex.getMessage());
            // TODO: Log
        }
        /*catch (ParserConfigurationException pce) {
            System.out.println(pce.getMessage());
        } catch (SAXException se) {
            System.out.println(se.getMessage());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }*/

        throw new MessagingEntityNotFoundException("Queue was not found");
    }

    static QueueDescription parseFromEntry(Node xEntry) {
        QueueDescription qd = null;
        NodeList nList = xEntry.getChildNodes();
        for (int i = 0; i < nList.getLength(); i++) {
            Node node = nList.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                Element element = (Element)node;
                switch(element.getTagName())
                {
                    case "title":
                        qd = new QueueDescription(element.getFirstChild().getNodeValue());
                        break;
                    case "content":
                        NodeList qdNodes = element.getFirstChild().getChildNodes();
                        for (int j = 0; j < qdNodes.getLength(); j++)
                        {
                            node = qdNodes.item(j);
                            if (node.getNodeType() == Node.ELEMENT_NODE) {
                                element = (Element) node;
                                switch (element.getTagName())
                                {
                                    case "MaxSizeInMegabytes":
                                        qd.maxSizeInMB = Long.parseLong(element.getFirstChild().getNodeValue());
                                        break;
                                    case "RequiresDuplicateDetection":
                                        qd.requiresDuplicateDetection = Boolean.parseBoolean(element.getFirstChild().getNodeValue());
                                        break;
                                    case "RequiresSession":
                                        qd.requiresSession = Boolean.parseBoolean(element.getFirstChild().getNodeValue());
                                        break;
                                    case "DeadLetteringOnMessageExpiration":
                                        qd.enableDeadLetteringOnMessageExpiration = Boolean.parseBoolean(element.getFirstChild().getNodeValue());
                                        break;
                                    case "DuplicateDetectionHistoryTimeWindow":
                                        qd.duplicationDetectionHistoryTimeWindow = Duration.parse(element.getFirstChild().getNodeValue());
                                        break;
                                    case "LockDuration":
                                        qd.lockDuration = Duration.parse(element.getFirstChild().getNodeValue());
                                        break;
                                    case "DefaultMessageTimeToLive":
                                        // TODO: Convert .net's MaxTimespan to Duration.Indefinite
                                        qd.defaultMessageTimeToLive = Duration.parse(element.getFirstChild().getNodeValue());
                                        break;
                                    case "MaxDeliveryCount":
                                        qd.maxDeliveryCount = Integer.parseInt(element.getFirstChild().getNodeValue());
                                        break;
                                    case "EnableBatchedOperations":
                                        qd.enableBatchedOperations = Boolean.parseBoolean(element.getFirstChild().getNodeValue());
                                        break;
                                    case "Status":
                                        //qd.status = (EntityStatus)Enum.Parse(typeof(EntityStatus), element.getFirstChild().getNodeValue());
                                        break;
                                    case "AutoDeleteOnIdle":
                                        qd.autoDeleteOnIdle = Duration.parse(element.getFirstChild().getNodeValue());
                                        break;
                                    case "EnablePartitioning":
                                        qd.enablePartitioning = Boolean.parseBoolean(element.getFirstChild().getNodeValue());
                                        break;
                                    case "UserMetadata":
                                        qd.userMetadata = element.getFirstChild().getNodeValue();
                                        break;
                                    case "ForwardTo":
                                        Node fwd = element.getFirstChild();
                                        if (fwd != null) {
                                            qd.forwardTo = fwd.getNodeValue();
                                        }
                                        break;
                                    case "ForwardDeadLetteredMessagesTo":
                                        Node fwdDlq = element.getFirstChild();
                                        if (fwdDlq != null) {
                                            qd.forwardDeadLetteredMessagesTo = fwdDlq.getNodeValue();
                                        }
                                        break;
                                    case "AuthorizationRules":
                                        //qd.AuthorizationRules = AuthorizationRules.ParseFromXElement(element);
                                        break;
                                }
                            }
                        }
                        break;
                }
            }
        }

        return qd;
    }
}
