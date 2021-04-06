package com.computablefacts.jupiter.iterators;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.VisibilityEvaluator;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
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
public class BlobStoreJsonFieldsAnonymizingIterator extends AnonymizingIterator {

  private static final String TYPE_JSON = Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL;

  private ObjectMapper mapper_;
  private JacksonJsonCore jsonCore_;

  public BlobStoreJsonFieldsAnonymizingIterator() {}

  @Generated
  @Override
  public IteratorOptions describeOptions() {

    Map<String, String> options = new HashMap<>();
    options.put("auths", "User authorizations.");

    return new IteratorOptions("BlobStoreJsonFieldsAnonymizingIterator",
        "BlobStoreJsonFieldsAnonymizingIterator filters out the JSON fields stored in the Accumulo Value for which the user does not have the right auths.",
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
  protected AnonymizingIterator create() {
    return new BlobStoreJsonFieldsAnonymizingIterator();
  }

  @Override
  protected void setTopKeyValue(Key key, Value value) {

    setTopKey(key);

    if (key.getColumnQualifier() == null
        || !key.getColumnQualifier().toString().startsWith(TYPE_JSON)
        || Constants.VALUE_ANONYMIZED.equals(value)) {
      setTopValue(value);
    } else {

      Set<String> auths = parsedAuths();
      String vizDataset = AbstractStorage.toVisibilityLabel(key.getColumnFamily().toString() + "_");
      String vizRawData = vizDataset + Constants.STRING_RAW_DATA;
      VisibilityEvaluator userVizEvaluator = visibilityEvaluator(vizRawData);

      if (auths.contains(vizRawData)
          && matches(userVizEvaluator, key.getColumnVisibilityParsed())) {

        // Here, the user has the <dataset>_RAW_DATA authorization : give him access to the full
        // JSON object
        setTopValue(value);
      } else {

        Map<String, Object> json = new JsonFlattener(jsonCore_, value.toString())
            .withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).flattenAsMap();

        // First, ensure the user has the right to visualize the returned fields
        json.keySet().removeIf(field -> {

          List<String> path = Lists.newArrayList(Splitter.on(Constants.SEPARATOR_CURRENCY_SIGN)
              .trimResults().omitEmptyStrings().split(field));

          return AbstractStorage.toVisibilityLabels(path).stream().map(label -> vizDataset + label)
              .noneMatch(auths::contains);
        });

        // Then, rebuild a new JSON object
        String newJson = new JsonUnflattener(jsonCore_, json)
            .withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).unflatten();

        // Next, set the new JSON object as the new Accumulo Value
        if ("{}".equals(newJson)) {
          setTopValue(Constants.VALUE_ANONYMIZED);
        } else {
          setTopValue(new Value(newJson.getBytes(StandardCharsets.UTF_8)));
        }
      }
    }
  }
}
