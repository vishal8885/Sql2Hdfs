/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sql2hdfs;

import java.io.File;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author tarun
 */
public class XMLData {

    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder;
    Document doc;
    Element rootElement;
    File f = new File("emps.xml");

    public XMLData() {

        try {
            dBuilder = dbFactory.newDocumentBuilder();
            if (f.exists()) {
                doc = dBuilder.parse(f);
                rootElement = (Element) doc.getDocumentElement();
            } else {
                doc = dBuilder.newDocument();
                rootElement = doc.createElementNS("Sql2Hdfs", "Users");
                doc.appendChild(rootElement);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean addUser(Property user) {
        rootElement.appendChild(getUser(doc, user.getDriver(), user.getUrl(), user.getUser(), user.getPass()));
        if (save()) {
            return true;
        }
        return false;
    }

    private boolean save() {
        try {
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            //transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            DOMSource source = new DOMSource(doc);
            StreamResult file = new StreamResult(f);
            transformer.transform(source, file);
            return true;
        } catch (TransformerConfigurationException ex) {
            return false;
        } catch (TransformerException ex) {
            return false;
        }
    }

    private static Node getUser(Document doc, String driver, String url, String username, String pass) {
        Element user = doc.createElement("User");
        user.appendChild(getUserElements(doc, user, "driver", driver));
        user.appendChild(getUserElements(doc, user, "url", url));
        user.appendChild(getUserElements(doc, user, "user", username));
        user.appendChild(getUserElements(doc, user, "pass", pass));
        return user;
    }

    //utility method to create text node
    private static Node getUserElements(Document doc, Element element, String name, String value) {
        Element node = doc.createElement(name);
        node.appendChild(doc.createTextNode(value));
        return node;
    }

    public String[] getUrls() {
        NodeList nList = doc.getElementsByTagName("User");
        String res[] = new String[nList.getLength()+1];
        res[0]="select";
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                res[temp+1] = eElement.getElementsByTagName("url").item(0).getTextContent();
            }
        }
        if (res == null) {
            return new String[]{""};
        }
        return res;
    }

    public Property getUser(String url) {
        NodeList nList = doc.getElementsByTagName("User");
        Property p = new Property();
        String res;
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                res = eElement.getElementsByTagName("url").item(0).getTextContent();
                if (res.equals(url)) {
                    p.setDriver(eElement.getElementsByTagName("driver").item(0).getTextContent());
                    p.setUrl(eElement.getElementsByTagName("url").item(0).getTextContent());
                    p.setUser(eElement.getElementsByTagName("user").item(0).getTextContent());
                    p.setPass(eElement.getElementsByTagName("pass").item(0).getTextContent());
                }
            }
        }
        return p;
    }

    public boolean removeNode(String url) {
        NodeList nList = doc.getElementsByTagName("User");
        String res;
        for (int temp = 0; temp < nList.getLength(); temp++) {
            Node nNode = nList.item(temp);
            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                res = eElement.getElementsByTagName("url").item(0).getTextContent();
                if (res.equals(url)) {
                    rootElement.removeChild(nNode);
                    save();
                    return true;
                }
            }
        }
        return false;
    }
}
