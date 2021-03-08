package tcep.graph.transition.mapek.contrast.testextensions;

import org.cardygan.fm.Attribute;
import org.cardygan.fm.FM;
import org.cardygan.fm.Feature;

import java.util.List;

/**
  * Created by Niels on 21.02.2018.
  */
public class FmUtils {

  public static List<Attribute> getAllAttributes(FM fm)  {
    return getAllAttributes(fm.getRoot());
  }

  public static List<Attribute> getAllAttributes(Feature feature) {
    List<Attribute> res = feature.getAttributes();
    feature.getChildren().forEach(f -> res.addAll(getAllAttributes(f)));
    return res;
  }

}
