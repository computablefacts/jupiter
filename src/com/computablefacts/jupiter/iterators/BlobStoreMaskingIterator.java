package com.computablefacts.jupiter.iterators;

import static com.computablefacts.jupiter.storage.Constants.SEPARATOR_CURRENCY_SIGN;
import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;
import static com.computablefacts.jupiter.storage.Constants.STRING_RAW_DATA;
import static com.computablefacts.nona.helpers.Document.ID_MAGIC_KEY;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.nona.Generated;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.base.JacksonJsonCore;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BlobStoreMaskingIterator extends MaskingIterator {

  private ObjectMapper mapper_;
  private JacksonJsonCore jsonCore_;

  public BlobStoreMaskingIterator() {}

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put("auths", "User authorizations.");
    options.put("salt", "User salt.");

    return new IteratorOptions("BlobStoreMaskingIterator",
        "BlobStoreMaskingIterator filters out the blobs stored in the Accumulo Value for which the user does not have the right auths.",
        options, null);
  }

  @Override
  public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options,
      IteratorEnvironment environment) {

    mapper_ = new ObjectMapper();
    mapper_.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    jsonCore_ = new JacksonJsonCore(mapper_);

    super.init(source, options, environment);
  }

  @Override
  protected MaskingIterator create() {
    return new BlobStoreMaskingIterator();
  }

  @Override
  protected void setTopKeyValue(Key key, Value value) {

    setTopKey(key);

    if (key.getColumnQualifier() == null || !Blob.isJson(key)) {

      setTopValue(value);

      // First, ensure the user has the ADM authorization
      Set<String> auths = parsedAuths();

      if (auths.contains(STRING_ADM)) {

        VisibilityEvaluator userVizEvaluator = visibilityEvaluator(STRING_ADM);
        ColumnVisibility rowViz = key.getColumnVisibilityParsed();

        // Then, ensure the row has the ADM authorization
        if (matches(userVizEvaluator, rowViz)) {

          // Next, ensure the ADM label is the only label that allows the user to have access to the
          // row data
          auths.remove(STRING_ADM);

          if (!matches(visibilityEvaluator(auths), rowViz)) {
            setTopValue(new Value(mask(salt(), value)));
          }
        }
      }
    } else {

      Set<String> auths = parsedAuths();
      String vizDataset = AbstractStorage.toVisibilityLabel(key.getColumnFamily().toString() + "_");
      String vizRawData = vizDataset + STRING_RAW_DATA;
      VisibilityEvaluator userVizEvaluator = visibilityEvaluator(vizRawData);

      if (auths.contains(vizRawData)
          && matches(userVizEvaluator, key.getColumnVisibilityParsed())) {

        // Here, the user has the <dataset>_RAW_DATA authorization : give him access to the full
        // JSON object
        setTopValue(value);
      } else {

        Map<String, Object> newJson = new HashMap<>();
        Map<String, Object> json = new JsonFlattener(jsonCore_, value.toString())
            .withSeparator(SEPARATOR_CURRENCY_SIGN).flattenAsMap();

        // First, ensure the user has the right to visualize the returned fields
        for (String field : json.keySet()) {

          Object object = json.get(field);
          List<String> path = Lists.newArrayList(
              Splitter.on(SEPARATOR_CURRENCY_SIGN).trimResults().omitEmptyStrings().split(field));

          if (ID_MAGIC_KEY.equals(field)) {
            newJson.put(field, object);
          } else if (AbstractStorage.toVisibilityLabels(path).stream()
              .map(label -> vizDataset + label).noneMatch(auths::contains)) {
            newJson.put(field, mask(salt(), object == null ? (String) object : object.toString()));
          } else {
            newJson.put(field, object);
          }
        }

        // Then, rebuild a new JSON object
        setTopValue(new Value(new JsonUnflattener(jsonCore_, newJson)
            .withSeparator(SEPARATOR_CURRENCY_SIGN).unflatten().getBytes(StandardCharsets.UTF_8)));
      }
    }
  }
}
