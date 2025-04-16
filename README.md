# Time Series QL

A high-performance time series database and query engine written in Rust, featuring a custom query language and REST API interface.

## Features

- **Fast Query Engine**: Built with Rust for optimal performance
- **REST API Interface**: Easy-to-use HTTP endpoints for data ingestion and querying
- **Custom Query Language**: Flexible time series querying capabilities
- **Arrow Integration**: Uses Apache Arrow for efficient data processing
- **Actor-based Architecture**: Leverages Actix for concurrent query processing
- **Real-time Data Ingestion**: Support for streaming time series data

## API Endpoints

### Health Check
```
GET /health
```
Returns the service health status.

### Data Ingestion
```
POST /ingest
```
Ingest time series data points with the following format:
```json
[
  {
    "timestamp": 1650123456789,
    "metric": "cpu_usage",
    "value": 75.5,
    "tags": "host=server1,env=prod"
  }
]
```

### Query
```
POST /query
```
Execute time series queries:
```json
{
  "query": "your_query_string",
  "start_time": 1650123456789,
  "end_time": 1650123456999
}
```

## Getting Started

### Prerequisites

- Rust (latest stable version)
- Cargo

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/time-series-ql.git
cd time-series-ql
```

2. Build the project:
```bash
cargo build --release
```

3. Run the server:
```bash
cargo run --release
```

The server will start on `http://127.0.0.1:8080`.

## Running Tests

Execute the test suite:
```bash
cargo test
```

## Architecture

The project is structured into several key components:

- `api`: REST API interface using Actix-web
- `engine`: Query execution engine
- `parser`: Query language parser
- `storage`: Time series data storage layer

## Performance

- Efficient data storage using Arrow's columnar format
- Concurrent query execution with actor-based processing
- Optimized batch processing for data ingestion

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built with [Actix-web](https://actix.rs/)
- Uses [Apache Arrow](https://arrow.apache.org/) for data processing
- Inspired by modern time series databases and query engines