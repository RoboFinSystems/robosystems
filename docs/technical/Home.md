# Welcome to the RoboSystems Wiki!

RoboSystems is an enterprise-grade financial knowledge graph platform that transforms complex financial data into actionable intelligence through graph-based analytics and AI-powered insights.

## 🚀 Getting Started

New to RoboSystems? Start here:

- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Load and query SEC financial filings
- **Quick Start**: See the [README](https://github.com/RoboFinSystems/robosystems#quick-start) for installation

## 📚 Core Documentation

### Architecture & Design

- **[Architecture Overview](Architecture-Overview)** - System design and components

### User Guides

- **[SEC XBRL Pipeline](SEC-XBRL-Pipeline)** - Working with SEC financial data
- **QuickBooks Integration** _(Coming soon)_
- **Custom Data Import** _(Coming soon)_

### Reference

- **Component READMEs**: Detailed technical docs in the [codebase](https://github.com/RoboFinSystems/robosystems/tree/main/robosystems)
  - [Graph API](/robosystems/graph_api/README.md)
  - [DuckDB Core](/robosystems/graph_api/core/README.md)
  - [Backend System](/robosystems/graph_api/backends/README.md)
  - [Processors](/robosystems/processors/README.md)
- **API Documentation**: [http://localhost:8000/docs](http://localhost:8000/docs) (local) or [https://api.robosystems.ai/docs](https://api.robosystems.ai/docs) (production)

## 🎯 Common Tasks

- **Load SEC Data**: `just sec-load NVDA 2024`
- **Query Data**: `just graph-query sec "MATCH (e:Entity) RETURN e LIMIT 10"`
- **Health Check**: `just sec-health`
- **View Logs**: `just logs graph-api`

## 💬 Support

- **Issues**: [GitHub Issues](https://github.com/RoboFinSystems/robosystems/issues)
- **Discussions**: [GitHub Discussions](https://github.com/RoboFinSystems/robosystems/discussions)
- **Main README**: [robosystems/README.md](https://github.com/RoboFinSystems/robosystems/blob/main/README.md)
