package com.computablefacts.jupiter.iterators;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import com.computablefacts.jupiter.storage.AbstractStorage;
import com.computablefacts.jupiter.storage.Constants;
import com.computablefacts.jupiter.storage.blobstore.Blob;
import com.computablefacts.nona.Generated;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.github.wnameless.json.unflattener.JsonUnflattener;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.errorprone.annotations.CheckReturnValue;

@CheckReturnValue
public class BlobStoreJsonFieldsAnonymizingIterator extends AnonymizingIterator {

  private static final String TYPE_JSON = Blob.TYPE_JSON + "" + Constants.SEPARATOR_NUL;

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

      String vizDataset = AbstractStorage.toVisibilityLabel(key.getColumnFamily().toString() + "_");
      Map<String, Object> json = new JsonFlattener(value.toString())
          .withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).flattenAsMap();

      // First, ensure the user has the right to visualize the returned fields
      Set<String> auths = parsedAuths();

      json.keySet().removeIf(field -> {

        List<String> path = Lists.newArrayList(Splitter.on(Constants.SEPARATOR_CURRENCY_SIGN)
            .trimResults().omitEmptyStrings().split(field));

        return AbstractStorage.toVisibilityLabels(path).stream().map(label -> vizDataset + label)
            .noneMatch(auths::contains);
      });

      // Then, rebuild a new JSON object
      String newJson =
          new JsonUnflattener(json).withSeparator(Constants.SEPARATOR_CURRENCY_SIGN).unflatten();

      // Next, set the new JSON object as the new Accumulo Value
      if ("{}".equals(newJson)) {
        setTopValue(Constants.VALUE_ANONYMIZED);
      } else {
        setTopValue(new Value(newJson.getBytes(StandardCharsets.UTF_8)));
      }
    }
  }
}
