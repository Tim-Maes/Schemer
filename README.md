# 🎯 Schemer - Database Schema Diff Tool

A powerful, single-file C# application that compares database schemas across different environments and generates migration scripts. Supports PostgreSQL, MySQL, SQL Server, and SQLite.

## ✨ Features

- **Lightning-fast** schema comparison across all major databases
- **Beautiful, colorful** console output with rich formatting
- **Multiple output formats**: Console, SQL, JSON, Markdown
- **Comprehensive change detection**: tables, columns, indexes, constraints
- **Production-ready** migration script generation
- **Zero-installation** single-file deployment

## 🚀 Quick Start

**1. Copy the Schemer.cs file to your local machine.**

**2. Run a schema comparison:**

Example using the test databases provided in this repository:

   ```bash
   dotnet run schemer.cs --source "Data Source=schemer_source.db" --target "Data Source=schemer_target.db" --type sqlite
   ```

**3. Example**

<img width="1994" height="1752" alt="image" src="https://github.com/user-attachments/assets/aa68e281-ac71-468a-baaf-fb15de5acf15" />


## 📋 Command Line Options

| Option | Description | Required | Default |
|--------|-------------|----------|---------|
| `--source` | Source database connection string | ✅ | - |
| `--target` | Target database connection string | ✅ | - |
| `--type` | Database type (postgres, mysql, sqlserver, sqlite) | ✅ | - |
| `--output` | Output format (console, sql, json, markdown) | ❌ | console |
| `--tables` | Comma-separated list of specific tables to compare | ❌ | all tables |
| `--ignore` | Comma-separated list of tables/columns to ignore | ❌ | none |
| `--migration-name` | Name for generated migration file | ❌ | auto-generated |
| `--verbose` | Detailed output including debug information | ❌ | false |

## 📊 Output Formats

### Console (Default)
Beautiful, colorful console output with rich formatting showing:
- Summary statistics
- Missing, extra, and modified tables
- Detailed column-level differences

### SQL Migration Scripts
Generate production-ready migration scripts:
```bash
./schemer.cs ... --output sql --migration-name "update_user_schema"
```

### JSON Reports
Machine-readable JSON format for integration with other tools:
```bash
./schemer.cs ... --output json
```

### Markdown Documentation
Human-readable documentation format:
```bash
./schemer.cs ... --output markdown
```

## 🔧 Connection String Examples

### PostgreSQL
```
postgres://username:password@hostname:5432/database
Host=hostname;Database=database;Username=username;Password=password
```

### MySQL
```
mysql://username:password@hostname:3306/database
Server=hostname;Database=database;Uid=username;Pwd=password
```

### SQL Server
```
Server=hostname;Database=database;Integrated Security=true
Server=hostname;Database=database;User Id=username;Password=password
```

### SQLite
```
Data Source=database.db
Data Source=C:\path\to\database.sqlite
```

## 🛠️ Requirements

- .NET 8.0 or later
- Database-specific drivers (automatically included via NuGet packages)

## 📦 Dependencies

The following NuGet packages are automatically included:
- `Npgsql` - PostgreSQL driver
- `MySqlConnector` - MySQL driver  
- `Microsoft.Data.SqlClient` - SQL Server driver
- `Microsoft.Data.Sqlite` - SQLite driver
- `Dapper` - Micro ORM
- `Spectre.Console` - Rich console UI
- `System.CommandLine` - Command line parsing
- `Newtonsoft.Json` - JSON serialization

## 🔒 Security Features

- **Connection string validation** with basic SQL injection prevention
- **Credential masking** in console output
- **Timeout handling** for long-running operations
- **Retry logic** with exponential backoff


## 🤝 Contributing

This is a single-file application designed for simplicity and portability. Contributions are welcome! Please ensure any changes maintain the single-file architecture.

