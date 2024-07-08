# Python Pandas MongoDB Helper

[![Pandas](https://img.shields.io/badge/Pandas-blue.svg?logo=pandas)][pandas-url]
[![MongoDB](https://img.shields.io/badge/MongoDB-green.svg?logo=mongodb)][mongodb-url]

This tool provides a set of utility methods to simplify working with [Pandas DataFrame][pandas-url]
and [MongoDB database][mongodb-url]. It aims to streamline common CRUD tasks, making it easier for developers to
build data-related pipelines, applications and services.

## Table of Contents

- [Features](#features)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [License](#license)
- [Contact](#contact)

## Features

* **Upsert DataFrame to MongoDB**: Easily update or insert Pandas DataFrame to MongoDB collections.
* **Get DataFrame from MongoDB**: Retrieve data from MongoDB collections and convert it into Pandas DataFrame.
* **Batch Processing**: Efficiently process large DataFrame by performing bulk MongoDB operations.

## Getting Started

### Prerequisites

* Python ≥ 3.8
* MongoDB server (Locally hosted or MongoDB Atlas)

### Installation

1. Git clone the repo or add the repo as submodule

2. Install required packages with the package manager [pip][pip-url]

```
pip install -r requirements.txt
```

## Usage

Sample code for conceptualization:

```
# Get dataframe
helper = MongoDBHelper(uri)
helper.get_df(database, collection)


# Upsert dataframe
helper = MongoDBHelper(uri)
helper.upsert_df(df, database, collection, '_id')
```

For entire examples, please refer to the [`examples` directory](examples).

## License

Distributed under the [Apache License 2.0](LICENSE).

## Contact

Project Link: [GitHub][project-url]

[mongodb-url]: https://www.mongodb.com
[pandas-url]: https://pandas.pydata.org
[pip-url]: https://pip.pypa.io/en/stable/
[project-url]: https://github.com/bosco-l/python-pandas-mongodb-helper

[Back to Top](#table-of-contents)