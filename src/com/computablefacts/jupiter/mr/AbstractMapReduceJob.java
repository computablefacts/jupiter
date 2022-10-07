package com.computablefacts.jupiter.mr;

import static com.computablefacts.jupiter.storage.Constants.STRING_ADM;

import com.computablefacts.jupiter.Configurations;
import com.computablefacts.logfmt.LogFormatter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.errorprone.annotations.CheckReturnValue;
import java.io.IOException;
import java.util.Date;
import java.util.Set;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@CheckReturnValue
public abstract class AbstractMapReduceJob implements Tool {

  private static final int MAX_MAPPERS = 3; // max. number of mappers to run in parallel

  private static final Logger logger_ = LoggerFactory.getLogger(AbstractMapReduceJob.class);

  private final String jobName_;
  private final Configurations input_;
  private final String inputTableName_;
  private final Configurations output_;
  private final String outputTableName_;

  private Configuration conf_;

  public AbstractMapReduceJob(String jobName, Configurations input, String inputTableName, Configurations output,
      String outputTableName) {

    Preconditions.checkArgument(!Strings.isNullOrEmpty(jobName), "jobName should neither be null nor empty");
    Preconditions.checkNotNull(input, "input should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(inputTableName),
        "inputTableName should neither be null nor empty");
    Preconditions.checkNotNull(output, "output should not be null");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(outputTableName),
        "outputTableName should neither be null nor empty");

    jobName_ = jobName;
    input_ = input;
    inputTableName_ = inputTableName;
    output_ = output;
    outputTableName_ = outputTableName;
  }

  @Override
  public int run(String[] args) {

    Preconditions.checkNotNull(args, "args should not be null");

    try {

      Job job = Job.getInstance(getConf());
      job.setJobName(jobName_);
      job.setJarByClass(getClass());

      // Setup source
      job.setInputFormatClass(AccumuloInputFormat.class);

      AccumuloInputFormat.setConnectorInfo(job, input_.username(), new PasswordToken(input_.password()));
      AccumuloInputFormat.setZooKeeperInstance(job,
          new ClientConfiguration().withInstance(input_.instanceName()).withZkHosts(input_.zooKeepers()));
      AccumuloInputFormat.setInputTableName(job, inputTableName_);
      AccumuloInputFormat.setScanAuthorizations(job, scanAuthorizations());
      // AccumuloInputFormat.setRanges(job, scanRanges(MAX_MAPPERS));
      AccumuloInputFormat.setAutoAdjustRanges(job, false); // ensure 1 mapper per declared range

      setupAccumuloInput(job);

      // Setup destination
      job.setOutputFormatClass(AccumuloOutputFormat.class);

      AccumuloOutputFormat.setCreateTables(job, false);
      AccumuloOutputFormat.setConnectorInfo(job, output_.username(), new PasswordToken(output_.password()));
      AccumuloOutputFormat.setZooKeeperInstance(job,
          new ClientConfiguration().withInstance(output_.instanceName()).withZkHosts(output_.zooKeepers()));
      AccumuloOutputFormat.setDefaultTableName(job, outputTableName_);

      setupAccumuloOutput(job);

      // Set input / output of the particular job
      job.setMapOutputKeyClass(Key.class);
      job.setMapOutputValueClass(Value.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Mutation.class);

      // Set mapper and reducer classes
      job.setMapperClass(mapper());
      job.setReducerClass(reducer());

      Date beginTime = new Date();

      logger_.info(LogFormatter.create(true)
          .message(String.format("Job \"%s\" for table \"%s\" started: %s", jobName_, inputTableName_, beginTime))
          .formatInfo());

      int exitCode = job.waitForCompletion(true) ? 0 : 1;

      if (exitCode != 0) {

        logger_.error(LogFormatter.create(true)
            .message(String.format("Job \"%s\" for table \"%s\" failed.", jobName_, inputTableName_)).formatError());

        return 1;
      }

      Date endTime = new Date();

      logger_.info(LogFormatter.create(true)
          .message(String.format("Job \"%s\" for table \"%s\" finished: %s", jobName_, inputTableName_, endTime))
          .formatInfo());

      logger_.info(LogFormatter.create(true).message(
          String.format("The job \"%s\" took %s seconds to complete.", jobName_,
              (endTime.getTime() - beginTime.getTime()) / 1000)).formatInfo());

      return 0;
    } catch (IOException | InterruptedException | ClassNotFoundException | AccumuloSecurityException e) {
      logger_.error(LogFormatter.create(true).message(e).formatError());
    }
    return 1;
  }

  @Override
  public Configuration getConf() {
    if (conf_ == null) {
      conf_ = new Configuration();
    }
    return conf_;
  }

  @Override
  public void setConf(Configuration conf) {
    conf_ = conf;
  }

  protected Authorizations scanAuthorizations() {
    return new Authorizations(STRING_ADM);
  }

  protected Set<Range> scanRanges(int maxMappers)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
    return scanRanges(new Range(), maxMappers);
  }

  protected Set<Range> scanRanges(Range range, int maxMappers)
      throws AccumuloException, AccumuloSecurityException, TableNotFoundException {

    Preconditions.checkNotNull(range, "range should not be null");
    Preconditions.checkArgument(maxMappers > 0, "maxMappers must be > 0");

    return input_.connector().tableOperations().splitRangeByTablets(inputTableName_, range, maxMappers);
  }

  protected void setupAccumuloInput(Job job) {
  }

  protected void setupAccumuloOutput(Job job) {
  }

  protected abstract Class<? extends Mapper> mapper();

  protected abstract Class<? extends Reducer> reducer();
}
