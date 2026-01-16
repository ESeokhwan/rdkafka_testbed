## Running Applications

The applications are configured via command-line arguments. For example, to run the V2X experiment consumer, you can use the following command:

```bash
./bin/v2x_expr_consumer --broker <your_kafka_broker> --topic-prefix <topic_prefix> [OPTIONS]
```

### `v2x_expr_consumer` Options

| Option                  | Description                                            | Default      |
| ----------------------- | ------------------------------------------------------ | ------------ |
| `--help`                | Show the help message.                                 |              |
| `--broker`              | Kafka broker address.                                  | (required)   |
| `--group-prefix`        | Prefix for the consumer group ID.                      | ""           |
| `--topic-prefix`        | Prefix for the topics to subscribe to.                 | ""           |
| `--client-cnt`          | Number of consumer clients to create.                  | 1            |
| `--running-time`        | Total running time for the test in milliseconds.       | 10000        |
| `--start-barrier-delay` | Delay in milliseconds before consumers start fetching. | 2000         |
| `--scrapable`           | Enable scrapable monitoring.                           | off          |
| `--read-tagged-only`    | Enable log sampling.                                   | off          |
| `--outdir`              | Directory to save output log files.                    | ""           |
| `--out-prefix`          | Prefix for output log file names.                      | ""           |
| `--verbose`             | Enable verbose logging.                                | off          |

For other applications, you can typically find usage instructions by running them with a `--help` flag if implemented.