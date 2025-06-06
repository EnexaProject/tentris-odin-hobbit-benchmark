# Tentris-Odin-Hobbit Benchmark Adaptor

This repository provides an adaptor to integrate the [Enexa Tentris](https://github.com/EnexaProject/tentris) module with the ODIN benchmark on the [HOBBIT](https://project-hobbit.eu/) benchmarking platform. It enables standardized performance evaluation of the Tentris RDF engine using the ODIN benchmark within the HOBBIT ecosystem.

## Features

- 🔌 **Adaptor Integration**: Connects the Enexa Tentris RDF engine with the ODIN benchmark.
- 🧪 **Benchmark Compatibility**: Fully compatible with the HOBBIT benchmarking platform.
- 🐳 **Docker Support**: Includes Docker setup for easy deployment and testing.
- ☕ **Java-Based**: Developed in Java for seamless integration with HOBBIT and Tentris.

## Repository Structure

- `src/`: Java source code for the ODIN benchmark adaptor.
- `Dockerfile`: Configuration to build the benchmark environment container.
- `README.md`: Project documentation.

## Prerequisites

- Java 8 or higher
- Maven
- Docker
- Access to HOBBIT benchmarking environment
- [Tentris](https://github.com/EnexaProject/tentris)

## Building and Running

To build the adaptor:

```bash
mvn clean install
