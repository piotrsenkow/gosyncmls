# GoSyncMLS!
![gosync](https://github.com/piotrsenkow/gosyncmls/assets/25436323/737b8034-4f08-4eec-a5bf-2f400cc6751d)
A Go application designed to fetch historical property listings and periodically update your local database with new listings from the MLS Grid API.
## Features

- **Data Synchronization**: Keeps local property listings in sync with the latest data from MLS Grid.
- **Error Handling**: Implements retries on API request failures, ensuring data consistency.
- **Rate Limiting**: Monitors and respects API usage limits to prevent overuse.
- **Concurrent Processing**: Efficiently processes data using Goroutines, ensuring optimal performance.
- **Logging**: Provides detailed logs for monitoring and debugging purposes.

## Why Go?

- **Concurrency**: Go's built-in Goroutines and channels make concurrent processing straightforward.
- **Performance**: Go offers fast execution and efficient memory usage.
- **Simplicity**: The language's clean syntax allows for quick development and easy maintenance.
- **Static Typing**: Helps catch type-related errors early in the development process.

## Getting Started

### Prerequisites

- Install [Go](https://golang.org/doc/install) (version 1.15 or higher recommended).
- A functional database setup (e.g., PostgreSQL).
- An active MLS Grid account to obtain the API bearer token.

### Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/piotrsenkow/gosyncmls.git
    cd gosyncmls
    ```

2. Install the required Go packages:
    ```bash
    go mod download
    ```

3. Setup required database by running the `db_schema.sql` script in an SQL shell or editor. Test connection. The go program will not run if it cannot find the database and its tables.

### Configuration

Set up the required environment variables:

- `API_BEARER_TOKEN`: Your MLS Grid API bearer token. Obtain one by applying for a data license at [mlsgrid.com](https://mlsgrid.com).
- `DB_CONN_STRING`: Your postgreSQL connection string. Example: `postgresql://user:password@localhost:5432/mred`

### Running the Application

Execute the command `go run main.go` from the project directory.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.
