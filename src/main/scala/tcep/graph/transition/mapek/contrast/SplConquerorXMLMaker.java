package tcep.graph.transition.mapek.contrast;

import org.cardygan.fm.*;
import org.cardygan.fm.impl.IntImpl;
import org.cardygan.fm.impl.RealImpl;
import org.cardygan.fm.util.FmUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplConquerorXMLMaker {

   public static void makeXML(FM fm, String vmName, File outputFolder, File resourcesFolder)
           throws ParserConfigurationException, TransformerException, IOException
   {

      DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
      DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

      Document doc = docBuilder.newDocument();
      Element vm = doc.createElement("vm");
      doc.appendChild(vm);
      vm.setAttribute("name", vmName);

      Element binaryOptions = doc.createElement("binaryOptions");
      vm.appendChild(binaryOptions);
      Element numericOptions = doc.createElement("numericOptions");
      vm.appendChild(numericOptions);
      Element booleanConstraints = doc.createElement("booleanConstraints");
      vm.appendChild(booleanConstraints);
      Element nonBooleanConstraints = doc.createElement("nonBooleanConstraints");
      vm.appendChild(nonBooleanConstraints);
      Element mixedConstraints = doc.createElement("mixedConstraints");
      vm.appendChild(mixedConstraints);

      Element configurationOption;

      Element name;
      Element outputString;
      Element prefix;
      Element postfix;
      Element parent;
      Element impliedOptions;
      Element excludedOptions;
      Element optional;

      Element options;

      Element minValue;
      Element maxValue;
      Element stepFunction;

      List<Feature> allFeatures = FmUtil.getAllFeatures(fm);
      // Map containing attributes and their parents name
      Map<Attribute, String> attributes = new HashMap<Attribute, String>();

      // Scaling Map:
      Map<String, Integer> attributeScalingMap = new HashMap<String, Integer>();
      //BufferedReader scalingReader = new BufferedReader(
      //        new FileReader(new File(resourcesFolder,"parameter_scaling_map.csv")));
      //String line = null;
      //scalingReader.readLine();
      //while ((line = scalingReader.readLine()) != null) {
      //   attributeScalingMap.put(line.split(";")[0], Integer.parseInt(line.split(";")[1]));
      //}
      //scalingReader.close();

      for (Feature currentFeature : allFeatures) {

         configurationOption = doc.createElement("configurationOption");
         binaryOptions.appendChild(configurationOption);

         name = doc.createElement("name");
         outputString = doc.createElement("outputString");
         prefix = doc.createElement("prefix");
         postfix = doc.createElement("postfix");
         parent = doc.createElement("parent");
         impliedOptions = doc.createElement("impliedOptions");
         excludedOptions = doc.createElement("excludedOptions");
         optional = doc.createElement("optional");

         name.appendChild(doc.createTextNode(currentFeature.getName()));
         configurationOption.appendChild(name);
         configurationOption.appendChild(outputString);
         configurationOption.appendChild(prefix);
         configurationOption.appendChild(postfix);
         if (!fm.getRoot().getName().equals(currentFeature.getName())) {
            parent.appendChild(doc.createTextNode(currentFeature.getParent().getName()));
         }
         configurationOption.appendChild(parent);
         configurationOption.appendChild(impliedOptions);
         if (currentFeature.getParent() != null && currentFeature.getParent().getGroupTypeCardinality() != null
                 && currentFeature.getParent().getGroupTypeCardinality().getLowerBound() == 1
                 && currentFeature.getParent().getGroupTypeCardinality().getUpperBound() == 1) {
            for (Feature childFeature : currentFeature.getParent().getChildren()) {
               if (!childFeature.getName().equals(currentFeature.getName())) {
                  options = doc.createElement("option");
                  options.appendChild(doc.createTextNode(childFeature.getName()));
                  excludedOptions.appendChild(options);
               }
            }
         }
         configurationOption.appendChild(excludedOptions);
         if (currentFeature.getFeatureInstanceCardinality().getLowerBound() == 0) {
            optional.appendChild(doc.createTextNode("True"));
         } else {
            optional.appendChild(doc.createTextNode("False"));
         }
         configurationOption.appendChild(optional);

         for (Attribute attr : currentFeature.getAttributes()) {
            attributes.put(attr, currentFeature.getName());
         }

      }

      for (Map.Entry<Attribute, String> entry : attributes.entrySet()) {
         Attribute attr = entry.getKey();
         String parentName = entry.getValue();

         configurationOption = doc.createElement("configurationOption");
         numericOptions.appendChild(configurationOption);

         name = doc.createElement("name");
         outputString = doc.createElement("outputString");
         prefix = doc.createElement("prefix");
         postfix = doc.createElement("postfix");
         parent = doc.createElement("parent");
         impliedOptions = doc.createElement("impliedOptions");
         excludedOptions = doc.createElement("excludedOptions");
         minValue = doc.createElement("minValue");
         maxValue = doc.createElement("maxValue");
         stepFunction = doc.createElement("stepFunction");

         name.appendChild(doc.createTextNode(attr.getName()));
         configurationOption.appendChild(name);
         configurationOption.appendChild(outputString);
         configurationOption.appendChild(prefix);
         configurationOption.appendChild(postfix);
         parent.appendChild(doc.createTextNode(parentName));
         configurationOption.appendChild(parent);
         configurationOption.appendChild(impliedOptions);
         configurationOption.appendChild(excludedOptions);

         // change minValue to 0 if it equals 1
         if(attr.getDomain().getClass() == RealImpl.class) {

            double minValueString = ((Real) attr.getDomain()).getBounds().getLb();
            if (minValueString == 1)
               minValueString = 0;
            minValue.appendChild(doc.createTextNode(Double.toString(minValueString)));
            configurationOption.appendChild(minValue);

            // Scale maxValue if contained in scaling map
            double maxValueReal = ((Real) attr.getDomain()).getBounds().getUb();
            if (attributeScalingMap.containsKey(attr.getName()))
               maxValueReal *= attributeScalingMap.get(attr.getName());

            maxValue.appendChild(doc.createTextNode(Double.toString(maxValueReal)));
            configurationOption.appendChild(maxValue);

         } else if(attr.getDomain().getClass() == IntImpl.class) {

            int minValueString = ((Int) attr.getDomain()).getBounds().getLb();

            if (minValueString == 1)
               minValueString = 0;
            minValue.appendChild(doc.createTextNode(Integer.toString(minValueString)));
            configurationOption.appendChild(minValue);

            // Scale maxValue if contained in scaling map
            int maxValueInt = ((Int) attr.getDomain()).getBounds().getUb();

            if (attributeScalingMap.containsKey(attr.getName()))
               maxValueInt *= attributeScalingMap.get(attr.getName());

            maxValue.appendChild(doc.createTextNode(Integer.toString(maxValueInt)));
            configurationOption.appendChild(maxValue);

         }

         // lambda function
         // makedigits(2, 5) -> 0.05
         java.util.function.BiFunction<Integer, Integer, String> makedigits = (n, last) -> {

            if(n > 0)
               return "0." + String.join("", Collections.nCopies(n - 1, "0")) + last.toString() ;
            else return last.toString();
         };

         if(attr.getDomain().getClass().equals(RealImpl.class)) {

            if(attr.getName().equals(FmNames.LOAD_VARIANCE()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.LOAD_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.JITTER()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.JITTER_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.NODECOUNT_CHANGERATE()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.NODECOUNT_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.NODE_TO_OP_RATIO()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.NODE_TO_OP_RATIO_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.AVG_EVENT_ARRIVAL_RATE()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.EVENT_FREQUENCY_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.EVENT_PUBLISHING_RATE()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.EVENT_FREQUENCY_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.NETWORK_USAGE()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.NETWORK_USAGE_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.AVG_VIV_DISTANCE()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.AVG_VIV_DISTANCE_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.VIV_DISTANCE_STDDEV()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.AVG_VIV_DISTANCE_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.GINI_CONNECTION_DEGREE_1_HOP()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.GINI_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.GINI_CONNECTION_DEGREE_2_HOPS()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.GINI_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.AVG_NODES_IN_1_HOP()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.GINI_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.AVG_NODES_IN_2_HOPS()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.GINI_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.AVG_HOPS_BETWEEN_NODES()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.GINI_DIGITS(), 1)));

            else if(attr.getName().equals(FmNames.MAX_PUB_TO_CLIENT_PING()))
               stepFunction.appendChild(doc.createTextNode(attr.getName() + " + " + makedigits.apply(FmNames.LATENCY_DIGITS(), 1)));

            else stepFunction.appendChild(doc.createTextNode(attr.getName() + " + 1"));
         }

         else if(attr.getDomain().getClass().equals(IntImpl.class)) {

            stepFunction.appendChild(doc.createTextNode(attr.getName() + " + 1"));
         }

         configurationOption.appendChild(stepFunction);

      }

      // write the content into xml file
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      DOMSource source = new DOMSource(doc);
      StreamResult result = new StreamResult(new File(outputFolder, "cfm.xml"));
      transformer.transform(source, result);

   }

}
