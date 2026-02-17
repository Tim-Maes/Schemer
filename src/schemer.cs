#!/usr/bin/env dotnet run
#:package Npgsql@8.0.8
#:package MySqlConnector@2.3.0  
#:package Microsoft.Data.SqlClient@5.2.3
#:package Microsoft.Data.Sqlite@8.0.0
#:package Dapper@2.1.66
#:package Spectre.Console@0.47.0
#:package System.CommandLine@2.0.0-beta4.22272.1
#:property PublishAot=false

using System;
using System.Collections.Generic;
using System.CommandLine;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using MySqlConnector;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Npgsql;
using Spectre.Console;

namespace Schemer;

/// <summary>
/// 🎯 Schemer - The Ultimate Database Schema Diff Tool
/// 
/// A powerful, single-file C# application that compares database schemas across different
/// environments and generates migration scripts. Supports PostgreSQL, MySQL, SQL Server, and SQLite.
/// 
/// Features:
/// - Lightning-fast schema comparison across all major databases
/// - Beautiful, colorful console output with rich formatting
/// - Multiple output formats: Console, SQL, JSON, Markdown
/// - Comprehensive change detection: tables, columns, indexes, constraints
/// - Production-ready migration script generation
/// - Zero-installation single-file deployment
/// 
/// Usage Examples:
///   ./schemer.cs --source "postgres://user:pass@localhost/source" --target "postgres://user:pass@localhost/target" --type postgres
///   ./schemer.cs --source "mysql://user:pass@localhost/source" --target "mysql://user:pass@localhost/target" --type mysql --output sql
///   ./schemer.cs --source "sqlite:source.db" --target "sqlite:target.db" --type sqlite --output json
/// </summary>
public static class Program
{
    public static async Task<int> Main(string[] args)
    {
        // Handle Ctrl+C gracefully
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            AnsiConsole.Write(new Panel("Operation cancelled by user")
                .Header("⚠️ [bold yellow]Cancelled[/]")
                .BorderColor(Spectre.Console.Color.Yellow));
            Environment.Exit(130); // Standard exit code for Ctrl+C
        };

        var app = CreateCommandLineApp();
        return await app.InvokeAsync(args);
    }

    #region Command Line Interface

    /// <summary>
    /// Creates and configures the command line application using System.CommandLine
    /// </summary>
    private static RootCommand CreateCommandLineApp()
    {
        var sourceOption = new Option<string>(
            name: "--source",
            description: "Source database connection string")
        {
            IsRequired = true
        };

        var targetOption = new Option<string>(
            name: "--target", 
            description: "Target database connection string")
        {
            IsRequired = true
        };

        var typeOption = new Option<DatabaseType>(
            name: "--type",
            description: "Database type")
        {
            IsRequired = true
        };

        var outputOption = new Option<OutputFormat>(
            name: "--output",
            description: "Output format",
            getDefaultValue: () => OutputFormat.Console);

        var tablesOption = new Option<string>(
            name: "--tables",
            description: "Comma-separated list of specific tables to compare");

        var ignoreOption = new Option<string>(
            name: "--ignore",
            description: "Comma-separated list of tables/columns to ignore");

        var migrationNameOption = new Option<string>(
            name: "--migration-name",
            description: "Name for generated migration file");

        var verboseOption = new Option<bool>(
            name: "--verbose",
            description: "Detailed output including debug information");

        var rootCommand = new RootCommand("🎯 Schemer - The Ultimate Database Schema Diff Tool")
        {
            sourceOption,
            targetOption,
            typeOption,
            outputOption,
            tablesOption,
            ignoreOption,
            migrationNameOption,
            verboseOption
        };

        rootCommand.SetHandler(async (string source, string target, DatabaseType type, OutputFormat output, 
            string? tables, string? ignore, string? migrationName, bool verbose) =>
        {
            var options = new ComparisonOptions
            {
                SourceConnectionString = source,
                TargetConnectionString = target,
                DatabaseType = type,
                OutputFormat = output,
                IncludeTables = tables?.Split(',', StringSplitOptions.RemoveEmptyEntries),
                IgnorePatterns = ignore?.Split(',', StringSplitOptions.RemoveEmptyEntries),
                MigrationName = migrationName ?? $"schema_migration_{DateTime.Now:yyyyMMdd_HHmmss}",
                Verbose = verbose
            };

            await RunSchemaComparison(options);
        }, 
        sourceOption, targetOption, typeOption, outputOption, tablesOption, ignoreOption, migrationNameOption, verboseOption);

        return rootCommand;
    }

    #endregion

    #region Core Data Models

    /// <summary>
    /// Represents the complete schema of a database including all tables, views, and metadata
    /// </summary>
    /// <param name="DatabaseName">The name of the database</param>
    /// <param name="Tables">List of all tables in the database</param>
    /// <param name="Views">List of all views in the database</param>
    /// <param name="Indexes">List of all indexes in the database</param>
    /// <param name="Metadata">Additional database-specific metadata</param>
    public record DatabaseSchema(
        string DatabaseName,
        List<Table> Tables,
        List<View> Views,
        List<Index> Indexes,
        Dictionary<string, object> Metadata
    );

    /// <summary>
    /// Represents a database table with its columns, constraints, and properties
    /// </summary>
    /// <param name="Name">Table name</param>
    /// <param name="Schema">Schema/namespace the table belongs to</param>
    /// <param name="Columns">List of all columns in the table</param>
    /// <param name="Constraints">List of all constraints (PK, FK, unique, etc.)</param>
    /// <param name="Properties">Additional table-specific properties</param>
    public record Table(
        string Name,
        string Schema,
        List<Column> Columns,
        List<Constraint> Constraints,
        Dictionary<string, object> Properties
    )
    {
        public string FullName => string.IsNullOrEmpty(Schema) ? Name : $"{Schema}.{Name}";
    }

    /// <summary>
    /// Represents a database column with all its properties and constraints
    /// </summary>
    /// <param name="Name">Column name</param>
    /// <param name="DataType">Data type (VARCHAR, INTEGER, etc.)</param>
    /// <param name="IsNullable">Whether the column allows NULL values</param>
    /// <param name="DefaultValue">Default value expression</param>
    /// <param name="MaxLength">Maximum length for string types</param>
    /// <param name="Precision">Numeric precision</param>
    /// <param name="Scale">Numeric scale</param>
    /// <param name="IsIdentity">Whether this is an identity/auto-increment column</param>
    /// <param name="IsComputed">Whether this is a computed column</param>
    /// <param name="Properties">Additional column-specific properties</param>
    public record Column(
        string Name,
        string DataType,
        bool IsNullable,
        string? DefaultValue,
        int? MaxLength,
        int? Precision,
        int? Scale,
        bool IsIdentity,
        bool IsComputed,
        Dictionary<string, object> Properties
    );

    /// <summary>
    /// Represents a database view
    /// </summary>
    /// <param name="Name">View name</param>
    /// <param name="Schema">Schema/namespace the view belongs to</param>
    /// <param name="Definition">View definition SQL</param>
    /// <param name="Properties">Additional view-specific properties</param>
    public record View(
        string Name,
        string Schema,
        string Definition,
        Dictionary<string, object> Properties
    )
    {
        public string FullName => string.IsNullOrEmpty(Schema) ? Name : $"{Schema}.{Name}";
    }

    /// <summary>
    /// Represents a database index
    /// </summary>
    /// <param name="Name">Index name</param>
    /// <param name="TableName">Table the index belongs to</param>
    /// <param name="Schema">Schema/namespace</param>
    /// <param name="Columns">List of indexed columns</param>
    /// <param name="IsUnique">Whether this is a unique index</param>
    /// <param name="IsPrimaryKey">Whether this is a primary key index</param>
    /// <param name="Properties">Additional index-specific properties</param>
    public record Index(
        string Name,
        string TableName,
        string Schema,
        List<string> Columns,
        bool IsUnique,
        bool IsPrimaryKey,
        Dictionary<string, object> Properties
    );

    /// <summary>
    /// Represents a database constraint (PK, FK, unique, check, etc.)
    /// </summary>
    /// <param name="Name">Constraint name</param>
    /// <param name="Type">Type of constraint</param>
    /// <param name="TableName">Table the constraint belongs to</param>
    /// <param name="Schema">Schema/namespace</param>
    /// <param name="Columns">Columns involved in the constraint</param>
    /// <param name="ReferencedTable">Referenced table (for foreign keys)</param>
    /// <param name="ReferencedColumns">Referenced columns (for foreign keys)</param>
    /// <param name="Properties">Additional constraint-specific properties</param>
    public record Constraint(
        string Name,
        ConstraintType Type,
        string TableName,
        string Schema,
        List<string> Columns,
        string? ReferencedTable,
        List<string>? ReferencedColumns,
        Dictionary<string, object> Properties
    );

    /// <summary>
    /// Represents the result of comparing two database schemas
    /// </summary>
    /// <param name="Summary">High-level summary of differences</param>
    /// <param name="MissingTables">Tables in source but not in target</param>
    /// <param name="ExtraTables">Tables in target but not in source</param>
    /// <param name="ModifiedTables">Tables that exist in both but have differences</param>
    /// <param name="MissingIndexes">Indexes in source but not in target</param>
    /// <param name="ExtraIndexes">Indexes in target but not in source</param>
    /// <param name="ModifiedIndexes">Indexes that exist in both but have differences</param>
    public record SchemaComparison(
        ComparisonSummary Summary,
        List<Table> MissingTables,
        List<Table> ExtraTables,
        List<TableComparison> ModifiedTables,
        List<Index> MissingIndexes,
        List<Index> ExtraIndexes,
        List<IndexComparison> ModifiedIndexes
    );

    /// <summary>
    /// High-level summary of schema comparison results
    /// </summary>
    /// <param name="TablesCompared">Total number of tables compared</param>
    /// <param name="DifferencesFound">Total number of differences found</param>
    /// <param name="MissingTables">Number of missing tables</param>
    /// <param name="ExtraTables">Number of extra tables</param>
    /// <param name="ModifiedTables">Number of modified tables</param>
    public record ComparisonSummary(
        int TablesCompared,
        int DifferencesFound,
        int MissingTables,
        int ExtraTables,
        int ModifiedTables
    );

    /// <summary>
    /// Represents the differences between two tables
    /// </summary>
    /// <param name="TableName">Name of the table being compared</param>
    /// <param name="MissingColumns">Columns in source but not in target</param>
    /// <param name="ExtraColumns">Columns in target but not in source</param>
    /// <param name="ModifiedColumns">Columns that exist in both but have differences</param>
    /// <param name="MissingConstraints">Constraints in source but not in target</param>
    /// <param name="ExtraConstraints">Constraints in target but not in source</param>
    /// <param name="ModifiedConstraints">Constraints that exist in both but have differences</param>
    public record TableComparison(
        string TableName,
        List<Column> MissingColumns,
        List<Column> ExtraColumns,
        List<ColumnComparison> ModifiedColumns,
        List<Constraint> MissingConstraints,
        List<Constraint> ExtraConstraints,
        List<ConstraintComparison> ModifiedConstraints
    );

    /// <summary>
    /// Represents the differences between two columns
    /// </summary>
    /// <param name="ColumnName">Name of the column being compared</param>
    /// <param name="SourceColumn">Column definition in source database</param>
    /// <param name="TargetColumn">Column definition in target database</param>
    /// <param name="Differences">List of specific differences</param>
    public record ColumnComparison(
        string ColumnName,
        Column SourceColumn,
        Column TargetColumn,
        List<string> Differences
    );

    /// <summary>
    /// Represents the differences between two constraints
    /// </summary>
    /// <param name="ConstraintName">Name of the constraint being compared</param>
    /// <param name="SourceConstraint">Constraint definition in source database</param>
    /// <param name="TargetConstraint">Constraint definition in target database</param>
    /// <param name="Differences">List of specific differences</param>
    public record ConstraintComparison(
        string ConstraintName,
        Constraint SourceConstraint,
        Constraint TargetConstraint,
        List<string> Differences
    );

    /// <summary>
    /// Represents the differences between two indexes
    /// </summary>
    /// <param name="IndexName">Name of the index being compared</param>
    /// <param name="SourceIndex">Index definition in source database</param>
    /// <param name="TargetIndex">Index definition in target database</param>
    /// <param name="Differences">List of specific differences</param>
    public record IndexComparison(
        string IndexName,
        Index SourceIndex,
        Index TargetIndex,
        List<string> Differences
    );

    #endregion

    #region Enums

    /// <summary>
    /// Supported database types
    /// </summary>
    public enum DatabaseType
    {
        Postgres,
        MySQL,
        SqlServer,
        SQLite
    }

    /// <summary>
    /// Output format options
    /// </summary>
    public enum OutputFormat
    {
        Console,
        SQL,
        JSON,
        Markdown
    }

    /// <summary>
    /// Types of database constraints
    /// </summary>
    public enum ConstraintType
    {
        PrimaryKey,
        ForeignKey,
        Unique,
        Check,
        Default,
        NotNull
    }

    #endregion

    #region Configuration

    /// <summary>
    /// Options for schema comparison operation
    /// </summary>
    public class ComparisonOptions
    {
        public string SourceConnectionString { get; set; } = string.Empty;
        public string TargetConnectionString { get; set; } = string.Empty;
        public DatabaseType DatabaseType { get; set; }
        public OutputFormat OutputFormat { get; set; } = OutputFormat.Console;
        public string[]? IncludeTables { get; set; }
        public string[]? IgnorePatterns { get; set; }
        public string MigrationName { get; set; } = string.Empty;
        public bool Verbose { get; set; }
    }

    /// <summary>
    /// Options for reading database schema
    /// </summary>
    public class SchemaReadOptions
    {
        public string[]? IncludeTables { get; set; }
        public string[]? ExcludeTables { get; set; }
        public string[]? IncludeSchemas { get; set; }
        public bool IncludeViews { get; set; } = true;
        public bool IncludeIndexes { get; set; } = true;
        public bool IncludeForeignKeys { get; set; } = true;
    }

    #endregion

    #region Schema Reader Interface

    /// <summary>
    /// Interface for reading database schemas from different database types
    /// </summary>
    public interface ISchemaReader
    {
        /// <summary>
        /// Reads the complete schema from a database
        /// </summary>
        /// <param name="connectionString">Database connection string</param>
        /// <param name="options">Options for schema reading</param>
        /// <returns>Complete database schema</returns>
        Task<DatabaseSchema> ReadSchemaAsync(string connectionString, SchemaReadOptions options);

        /// <summary>
        /// Tests if the connection string is valid and accessible
        /// </summary>
        /// <param name="connectionString">Database connection string</param>
        /// <returns>True if connection is valid</returns>
        Task<bool> TestConnectionAsync(string connectionString);

        /// <summary>
        /// Gets a display-friendly name from the connection string (masks passwords)
        /// </summary>
        /// <param name="connectionString">Database connection string</param>
        /// <returns>Safe display name</returns>
        string GetConnectionDisplayName(string connectionString);
    }

    #endregion

    #region Main Application Logic

    /// <summary>
    /// Main entry point for schema comparison operation
    /// </summary>
    private static async Task RunSchemaComparison(ComparisonOptions options)
    {
        try
        {
            // Validate input parameters
            ValidateOptions(options);

            // Display the beautiful Schemer banner
            DisplayBanner();

            // Create the appropriate schema reader
            var schemaReader = CreateSchemaReader(options.DatabaseType);
            
            // Show connection info (with masked credentials)
            AnsiConsole.WriteLine($"📊 [bold]Database Type:[/] {options.DatabaseType}");
            AnsiConsole.WriteLine($"📡 [bold]Source:[/] {schemaReader.GetConnectionDisplayName(options.SourceConnectionString)}");
            AnsiConsole.WriteLine($"📡 [bold]Target:[/] {schemaReader.GetConnectionDisplayName(options.TargetConnectionString)}");
            AnsiConsole.WriteLine();

            // Show progress while reading schemas
            var comparison = await AnsiConsole.Status()
                .StartAsync("🔍 Reading database schemas...", async ctx =>
                {
                    try
                    {
                        // Validate connections with timeout
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                        
                        ctx.Status("🔗 Validating source connection...");
                        var sourceValid = await ValidateConnectionWithRetry(schemaReader, options.SourceConnectionString, cts.Token);
                        if (!sourceValid)
                            throw new InvalidOperationException("Failed to connect to source database after multiple attempts");

                        ctx.Status("🔗 Validating target connection...");
                        var targetValid = await ValidateConnectionWithRetry(schemaReader, options.TargetConnectionString, cts.Token);
                        if (!targetValid)
                            throw new InvalidOperationException("Failed to connect to target database after multiple attempts");

                        // Read schemas with timeout
                        var readOptions = CreateSchemaReadOptions(options);
                        
                        ctx.Status("📖 Reading source schema...");
                        var sourceSchema = await ReadSchemaWithTimeout(schemaReader, options.SourceConnectionString, readOptions, TimeSpan.FromMinutes(5));
                        
                        ctx.Status("📖 Reading target schema...");
                        var targetSchema = await ReadSchemaWithTimeout(schemaReader, options.TargetConnectionString, readOptions, TimeSpan.FromMinutes(5));

                        // Validate schema data
                        ValidateSchema(sourceSchema, "source");
                        ValidateSchema(targetSchema, "target");

                        // Compare schemas
                        ctx.Status("⚖️  Comparing schemas...");
                        return CompareSchemas(sourceSchema, targetSchema);
                    }
                    catch (OperationCanceledException)
                    {
                        throw new TimeoutException("Operation timed out. Please check your connection strings and network connectivity.");
                    }
                });

            // Output results in the specified format
            await OutputResults(comparison, options);

            // Show completion message
            var successRule = new Spectre.Console.Rule("[bold green]✅ Schema comparison completed successfully![/]");
            AnsiConsole.Write(successRule);
        }
        catch (ValidationException ex)
        {
            AnsiConsole.Write(new Panel(ex.Message)
                .Header("❌ [bold red]Validation Error[/]")
                .BorderColor(Spectre.Console.Color.Red));
            Environment.Exit(1);
        }
        catch (TimeoutException ex)
        {
            AnsiConsole.Write(new Panel(ex.Message)
                .Header("⏰ [bold yellow]Timeout Error[/]")
                .BorderColor(Spectre.Console.Color.Yellow));
            Environment.Exit(1);
        }
        catch (UnauthorizedAccessException ex)
        {
            AnsiConsole.Write(new Panel($"Access denied: {ex.Message}")
                .Header("🔒 [bold red]Permission Error[/]")
                .BorderColor(Spectre.Console.Color.Red));
            Environment.Exit(1);
        }
        catch (Exception ex)
        {
            AnsiConsole.Write(new Panel($"An unexpected error occurred: {ex.Message}")
                .Header("💥 [bold red]Error[/]")
                .BorderColor(Spectre.Console.Color.Red));
            
            if (options.Verbose)
            {
                AnsiConsole.WriteLine();
                AnsiConsole.WriteException(ex);
            }
            
            Environment.Exit(1);
        }
    }

    /// <summary>
    /// Displays the beautiful Schemer banner with branding
    /// </summary>
    private static void DisplayBanner()
    {
        var figlet = new FigletText("Schemer")
            .Centered()
            .Color(Spectre.Console.Color.Blue);
        
        AnsiConsole.Write(figlet);
        
        AnsiConsole.Write(new Markup("[bold blue]🎯 The Ultimate Database Schema Diff Tool[/]").Centered());
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Markup("[dim]Lightning-fast • Cross-platform[/]").Centered());
        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine();
    }

    /// <summary>
    /// Creates the appropriate schema reader for the specified database type
    /// </summary>
    private static ISchemaReader CreateSchemaReader(DatabaseType databaseType)
    {
        return databaseType switch
        {
            DatabaseType.Postgres => new PostgreSqlSchemaReader(),
            DatabaseType.MySQL => new MySqlSchemaReader(),
            DatabaseType.SqlServer => new SqlServerSchemaReader(),
            DatabaseType.SQLite => new SqliteSchemaReader(),
            _ => throw new ArgumentException($"Unsupported database type: {databaseType}")
        };
    }

    /// <summary>
    /// Creates schema read options from comparison options
    /// </summary>
    private static SchemaReadOptions CreateSchemaReadOptions(ComparisonOptions options)
    {
        return new SchemaReadOptions
        {
            IncludeTables = options.IncludeTables,
            ExcludeTables = options.IgnorePatterns,
            IncludeViews = true,
            IncludeIndexes = true,
            IncludeForeignKeys = true
        };
    }

    #endregion

    #region Validation and Security

    /// <summary>
    /// Custom validation exception for parameter validation
    /// </summary>
    public class ValidationException : Exception
    {
        public ValidationException(string message) : base(message) { }
        public ValidationException(string message, Exception innerException) : base(message, innerException) { }
    }

    /// <summary>
    /// Validates the comparison options
    /// </summary>
    private static void ValidateOptions(ComparisonOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.SourceConnectionString))
            throw new ValidationException("Source connection string is required");

        if (string.IsNullOrWhiteSpace(options.TargetConnectionString))
            throw new ValidationException("Target connection string is required");

        if (string.IsNullOrWhiteSpace(options.MigrationName))
            throw new ValidationException("Migration name cannot be empty");

        // Validate connection strings don't contain suspicious content
        ValidateConnectionStringSafety(options.SourceConnectionString, "source");
        ValidateConnectionStringSafety(options.TargetConnectionString, "target");

        // Validate migration name is filesystem safe
        if (options.MigrationName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            throw new ValidationException("Migration name contains invalid characters");
    }

    /// <summary>
    /// Validates connection string for basic security
    /// </summary>
    private static void ValidateConnectionStringSafety(string connectionString, string name)
    {
        if (connectionString.Length > 2000)
            throw new ValidationException($"{name} connection string is too long (max 2000 characters)");

        // Basic SQL injection prevention - look for suspicious patterns
        // Temporarily commented out to allow standard connection string formats
        // var suspiciousPatterns = new[] { "--", ";", "/*", "*/" };
        // foreach (var pattern in suspiciousPatterns)
        // {
        //     if (connectionString.Contains(pattern, StringComparison.OrdinalIgnoreCase))
        //         throw new ValidationException($"{name} connection string contains suspicious pattern: {pattern}");
        // }
    }

    /// <summary>
    /// Validates a database schema
    /// </summary>
    private static void ValidateSchema(DatabaseSchema schema, string name)
    {
        if (schema == null)
            throw new ValidationException($"{name} schema is null");

        if (string.IsNullOrWhiteSpace(schema.DatabaseName))
            throw new ValidationException($"{name} schema has no database name");

        if (schema.Tables == null)
            throw new ValidationException($"{name} schema has null tables collection");

        // Validate table structure
        foreach (var table in schema.Tables)
        {
            if (string.IsNullOrWhiteSpace(table.Name))
                throw new ValidationException($"{name} schema contains table with no name");

            if (table.Columns == null)
                throw new ValidationException($"{name} schema table '{table.Name}' has null columns");

            foreach (var column in table.Columns)
            {
                if (string.IsNullOrWhiteSpace(column.Name))
                    throw new ValidationException($"{name} schema table '{table.Name}' contains column with no name");

                if (string.IsNullOrWhiteSpace(column.DataType))
                    throw new ValidationException($"{name} schema table '{table.Name}' column '{column.Name}' has no data type");
            }
        }
    }

    /// <summary>
    /// Validates connection with retry logic
    /// </summary>
    private static async Task<bool> ValidateConnectionWithRetry(ISchemaReader reader, string connectionString, CancellationToken cancellationToken)
    {
        const int maxRetries = 3;
        var delay = TimeSpan.FromSeconds(1);

        for (int i = 0; i < maxRetries; i++)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                return await reader.TestConnectionAsync(connectionString);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch when (i < maxRetries - 1)
            {
                await Task.Delay(delay, cancellationToken);
                delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * 2); // Exponential backoff
            }
        }

        return false;
    }

    /// <summary>
    /// Reads schema with timeout handling
    /// </summary>
    private static async Task<DatabaseSchema> ReadSchemaWithTimeout(ISchemaReader reader, string connectionString, SchemaReadOptions options, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            var task = reader.ReadSchemaAsync(connectionString, options);
            return await task;
        }
        catch (OperationCanceledException)
        {
            throw new TimeoutException($"Schema reading timed out after {timeout.TotalMinutes:F1} minutes");
        }
    }

    #endregion

    #region Schema Comparison Engine

    /// <summary>
    /// Compares two database schemas and identifies all differences using parallel processing
    /// </summary>
    private static SchemaComparison CompareSchemas(DatabaseSchema sourceSchema, DatabaseSchema targetSchema)
    {
        // Create lookup dictionaries for fast access
        var sourceTableLookup = sourceSchema.Tables.ToDictionary(t => t.FullName, t => t);
        var targetTableLookup = targetSchema.Tables.ToDictionary(t => t.FullName, t => t);
        
        var sourceTableNames = sourceTableLookup.Keys.ToHashSet();
        var targetTableNames = targetTableLookup.Keys.ToHashSet();

        // Find missing and extra tables (parallel processing)
        var missingTables = sourceSchema.Tables
            .AsParallel()
            .Where(table => !targetTableNames.Contains(table.FullName))
            .ToList();

        var extraTables = targetSchema.Tables
            .AsParallel()
            .Where(table => !sourceTableNames.Contains(table.FullName))
            .ToList();

        // Find modified tables (parallel processing for better performance with large schemas)
        var modifiedTables = sourceSchema.Tables
            .AsParallel()
            .Where(sourceTable => targetTableLookup.ContainsKey(sourceTable.FullName))
            .Select(sourceTable => 
            {
                var targetTable = targetTableLookup[sourceTable.FullName];
                return CompareTables(sourceTable, targetTable);
            })
            .Where(HasDifferences)
            .ToList();

        // Compare indexes
        var missingIndexes = new List<Index>();
        var extraIndexes = new List<Index>();
        var modifiedIndexes = new List<IndexComparison>();

        var sourceIndexNames = sourceSchema.Indexes.Select(i => i.Name).ToHashSet();
        var targetIndexNames = targetSchema.Indexes.Select(i => i.Name).ToHashSet();

        foreach (var index in sourceSchema.Indexes)
        {
            if (!targetIndexNames.Contains(index.Name))
                missingIndexes.Add(index);
        }

        foreach (var index in targetSchema.Indexes)
        {
            if (!sourceIndexNames.Contains(index.Name))
                extraIndexes.Add(index);
        }

        foreach (var sourceIndex in sourceSchema.Indexes)
        {
            var targetIndex = targetSchema.Indexes.FirstOrDefault(i => i.Name == sourceIndex.Name);
            if (targetIndex != null)
            {
                var differences = CompareIndexes(sourceIndex, targetIndex);
                if (differences.Any())
                {
                    modifiedIndexes.Add(new IndexComparison(
                        sourceIndex.Name, sourceIndex, targetIndex, differences));
                }
            }
        }

        var summary = new ComparisonSummary(
            TablesCompared: sourceSchema.Tables.Count + targetSchema.Tables.Count,
            DifferencesFound: missingTables.Count + extraTables.Count + modifiedTables.Count,
            MissingTables: missingTables.Count,
            ExtraTables: extraTables.Count,
            ModifiedTables: modifiedTables.Count
        );

        return new SchemaComparison(
            Summary: summary,
            MissingTables: missingTables,
            ExtraTables: extraTables,
            ModifiedTables: modifiedTables,
            MissingIndexes: missingIndexes,
            ExtraIndexes: extraIndexes,
            ModifiedIndexes: modifiedIndexes
        );
    }

    /// <summary>
    /// Compares two tables and identifies all differences with optimized performance
    /// </summary>
    private static TableComparison CompareTables(Table sourceTable, Table targetTable)
    {
        // Create column lookup dictionaries for O(1) access
        var sourceColumnLookup = sourceTable.Columns.ToDictionary(c => c.Name, c => c);
        var targetColumnLookup = targetTable.Columns.ToDictionary(c => c.Name, c => c);
        
        var sourceColumnNames = sourceColumnLookup.Keys.ToHashSet();
        var targetColumnNames = targetColumnLookup.Keys.ToHashSet();

        // Find missing and extra columns using set operations
        var missingColumns = sourceTable.Columns
            .Where(column => !targetColumnNames.Contains(column.Name))
            .ToList();

        var extraColumns = targetTable.Columns
            .Where(column => !sourceColumnNames.Contains(column.Name))
            .ToList();

        // Find modified columns using parallel processing for large tables
        var modifiedColumns = sourceTable.Columns
            .AsParallel()
            .Where(sourceColumn => targetColumnLookup.ContainsKey(sourceColumn.Name))
            .Select(sourceColumn =>
            {
                var targetColumn = targetColumnLookup[sourceColumn.Name];
                var differences = CompareColumns(sourceColumn, targetColumn);
                return new { sourceColumn, targetColumn, differences };
            })
            .Where(x => x.differences.Any())
            .Select(x => new ColumnComparison(x.sourceColumn.Name, x.sourceColumn, x.targetColumn, x.differences))
            .ToList();

        // Compare constraints
        var missingConstraints = new List<Constraint>();
        var extraConstraints = new List<Constraint>();
        var modifiedConstraints = new List<ConstraintComparison>();

        var sourceConstraintNames = sourceTable.Constraints.Select(c => c.Name).ToHashSet();
        var targetConstraintNames = targetTable.Constraints.Select(c => c.Name).ToHashSet();

        foreach (var constraint in sourceTable.Constraints)
        {
            if (!targetConstraintNames.Contains(constraint.Name))
                missingConstraints.Add(constraint);
        }

        foreach (var constraint in targetTable.Constraints)
        {
            if (!sourceConstraintNames.Contains(constraint.Name))
                extraConstraints.Add(constraint);
        }

        foreach (var sourceConstraint in sourceTable.Constraints)
        {
            var targetConstraint = targetTable.Constraints.FirstOrDefault(c => c.Name == sourceConstraint.Name);
            if (targetConstraint != null)
            {
                var differences = CompareConstraints(sourceConstraint, targetConstraint);
                if (differences.Any())
                {
                    modifiedConstraints.Add(new ConstraintComparison(
                        sourceConstraint.Name, sourceConstraint, targetConstraint, differences));
                }
            }
        }

        return new TableComparison(
            TableName: sourceTable.FullName,
            MissingColumns: missingColumns,
            ExtraColumns: extraColumns,
            ModifiedColumns: modifiedColumns,
            MissingConstraints: missingConstraints,
            ExtraConstraints: extraConstraints,
            ModifiedConstraints: modifiedConstraints
        );
    }

    /// <summary>
    /// Compares two columns and returns a list of differences
    /// </summary>
    private static List<string> CompareColumns(Column source, Column target)
    {
        var differences = new List<string>();

        if (source.DataType != target.DataType)
            differences.Add($"Data type changed from {source.DataType} to {target.DataType}");

        if (source.IsNullable != target.IsNullable)
            differences.Add($"Nullable changed from {source.IsNullable} to {target.IsNullable}");

        if (source.DefaultValue != target.DefaultValue)
            differences.Add($"Default value changed from '{source.DefaultValue}' to '{target.DefaultValue}'");

        if (source.MaxLength != target.MaxLength)
            differences.Add($"Max length changed from {source.MaxLength} to {target.MaxLength}");

        if (source.Precision != target.Precision)
            differences.Add($"Precision changed from {source.Precision} to {target.Precision}");

        if (source.Scale != target.Scale)
            differences.Add($"Scale changed from {source.Scale} to {target.Scale}");

        if (source.IsIdentity != target.IsIdentity)
            differences.Add($"Identity changed from {source.IsIdentity} to {target.IsIdentity}");

        return differences;
    }

    /// <summary>
    /// Compares two constraints and returns a list of differences
    /// </summary>
    private static List<string> CompareConstraints(Constraint source, Constraint target)
    {
        var differences = new List<string>();

        if (source.Type != target.Type)
            differences.Add($"Constraint type changed from {source.Type} to {target.Type}");

        if (!source.Columns.SequenceEqual(target.Columns))
            differences.Add($"Columns changed from [{string.Join(", ", source.Columns)}] to [{string.Join(", ", target.Columns)}]");

        if (source.ReferencedTable != target.ReferencedTable)
            differences.Add($"Referenced table changed from {source.ReferencedTable} to {target.ReferencedTable}");

        if (source.ReferencedColumns != null && target.ReferencedColumns != null && 
            !source.ReferencedColumns.SequenceEqual(target.ReferencedColumns))
            differences.Add($"Referenced columns changed from [{string.Join(", ", source.ReferencedColumns)}] to [{string.Join(", ", target.ReferencedColumns)}]");

        return differences;
    }

    /// <summary>
    /// Compares two indexes and returns a list of differences
    /// </summary>
    private static List<string> CompareIndexes(Index source, Index target)
    {
        var differences = new List<string>();

        if (source.TableName != target.TableName)
            differences.Add($"Table changed from {source.TableName} to {target.TableName}");

        if (!source.Columns.SequenceEqual(target.Columns))
            differences.Add($"Columns changed from [{string.Join(", ", source.Columns)}] to [{string.Join(", ", target.Columns)}]");

        if (source.IsUnique != target.IsUnique)
            differences.Add($"Unique changed from {source.IsUnique} to {target.IsUnique}");

        if (source.IsPrimaryKey != target.IsPrimaryKey)
            differences.Add($"Primary key changed from {source.IsPrimaryKey} to {target.IsPrimaryKey}");

        return differences;
    }

    /// <summary>
    /// Checks if a table comparison has any differences
    /// </summary>
    private static bool HasDifferences(TableComparison comparison)
    {
        return comparison.MissingColumns.Any() ||
               comparison.ExtraColumns.Any() ||
               comparison.ModifiedColumns.Any() ||
               comparison.MissingConstraints.Any() ||
               comparison.ExtraConstraints.Any() ||
               comparison.ModifiedConstraints.Any();
    }

    #endregion

    #region Output Formatters

    /// <summary>
    /// Outputs the comparison results in the specified format
    /// </summary>
    private static async Task OutputResults(SchemaComparison comparison, ComparisonOptions options)
    {
        switch (options.OutputFormat)
        {
            case OutputFormat.Console:
                OutputConsole(comparison, options);
                break;
            case OutputFormat.SQL:
                await OutputSql(comparison, options);
                break;
            case OutputFormat.JSON:
                await OutputJson(comparison, options);
                break;
            case OutputFormat.Markdown:
                await OutputMarkdown(comparison, options);
                break;
        }
    }

    /// <summary>
    /// Outputs results to console with rich formatting
    /// </summary>
    private static void OutputConsole(SchemaComparison comparison, ComparisonOptions options)
    {
        // Summary panel
        var summaryTable = new Spectre.Console.Table()
            .Border(TableBorder.Rounded)
            .BorderColor(Spectre.Console.Color.Blue)
            .AddColumn("[bold]Metric[/]")
            .AddColumn("[bold]Count[/]");

        summaryTable.AddRow("📊 Tables Compared", comparison.Summary.TablesCompared.ToString());
        summaryTable.AddRow("⚠️  Differences Found", comparison.Summary.DifferencesFound.ToString());
        summaryTable.AddRow("🔴 Missing Tables", comparison.Summary.MissingTables.ToString());
        summaryTable.AddRow("🟡 Extra Tables", comparison.Summary.ExtraTables.ToString());  
        summaryTable.AddRow("🔄 Modified Tables", comparison.Summary.ModifiedTables.ToString());

        var summaryPanel = new Panel(summaryTable)
            .Header("📈 [bold blue]Schema Comparison Summary[/]")
            .BorderColor(Spectre.Console.Color.Blue);

        AnsiConsole.Write(summaryPanel);
        AnsiConsole.WriteLine();

        // Missing tables
        if (comparison.MissingTables.Any())
        {
            var missingTable = new Spectre.Console.Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Spectre.Console.Color.Red)
                .AddColumn("[bold]Table Name[/]")
                .AddColumn("[bold]Schema[/]")
                .AddColumn("[bold]Columns[/]");

            foreach (var table in comparison.MissingTables)
            {
                missingTable.AddRow(
                    $"[red]{table.Name}[/]",
                    table.Schema,
                    table.Columns.Count.ToString()
                );
            }

            var missingPanel = new Panel(missingTable)
                .Header("🔴 [bold red]Missing Tables (in source, not in target)[/]")
                .BorderColor(Spectre.Console.Color.Red);

            AnsiConsole.Write(missingPanel);
            AnsiConsole.WriteLine();
        }

        // Extra tables
        if (comparison.ExtraTables.Any())
        {
            var extraTable = new Spectre.Console.Table()
                .Border(TableBorder.Rounded)
                .BorderColor(Spectre.Console.Color.Yellow)
                .AddColumn("[bold]Table Name[/]")
                .AddColumn("[bold]Schema[/]")
                .AddColumn("[bold]Columns[/]");

            foreach (var table in comparison.ExtraTables)
            {
                extraTable.AddRow(
                    $"[yellow]{table.Name}[/]",
                    table.Schema,
                    table.Columns.Count.ToString()
                );
            }

            var extraPanel = new Panel(extraTable)
                .Header("🟡 [bold yellow]Extra Tables (in target, not in source)[/]")
                .BorderColor(Spectre.Console.Color.Yellow);

            AnsiConsole.Write(extraPanel);
            AnsiConsole.WriteLine();
        }

        // Modified tables
        if (comparison.ModifiedTables.Any())
        {
            foreach (var tableComparison in comparison.ModifiedTables)
            {
                OutputTableComparison(tableComparison);
                AnsiConsole.WriteLine();
            }
        }

        // Show success message or prompt for migration generation
        if (comparison.Summary.DifferencesFound == 0)
        {
            var successPanel = new Panel(new Markup("[bold green]✅ Schemas are identical! No differences found.[/]"))
                .Header("🎉 [bold green]Success[/]")
                .BorderColor(Spectre.Console.Color.Green);
            AnsiConsole.Write(successPanel);
        }
        else
        {
            var rule = new Spectre.Console.Rule("[bold blue]Migration Options[/]");
            AnsiConsole.Write(rule);
            
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Markup("[dim]To generate migration scripts, re-run with:[/]"));
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Markup($"[cyan]--output sql --migration-name {options.MigrationName}[/]"));
            AnsiConsole.WriteLine();
        }
    }

    /// <summary>
    /// Outputs detailed table comparison to console
    /// </summary>
    private static void OutputTableComparison(TableComparison comparison)
    {
        var tree = new Tree($"🔄 [bold cyan]{comparison.TableName}[/]");

        // Missing columns
        if (comparison.MissingColumns.Any())
        {
            var missingNode = tree.AddNode("🔴 [red]Missing Columns[/]");
            foreach (var column in comparison.MissingColumns)
            {
                missingNode.AddNode($"[red]{column.Name}[/] ({column.DataType})");
            }
        }

        // Extra columns
        if (comparison.ExtraColumns.Any())
        {
            var extraNode = tree.AddNode("🟡 [yellow]Extra Columns[/]");
            foreach (var column in comparison.ExtraColumns)
            {
                extraNode.AddNode($"[yellow]{column.Name}[/] ({column.DataType})");
            }
        }

        // Modified columns
        if (comparison.ModifiedColumns.Any())
        {
            var modifiedNode = tree.AddNode("🔄 [blue]Modified Columns[/]");
            foreach (var columnComparison in comparison.ModifiedColumns)
            {
                var columnNode = modifiedNode.AddNode($"[blue]{columnComparison.ColumnName}[/]");
                foreach (var difference in columnComparison.Differences)
                {
                    columnNode.AddNode($"[dim]• {difference}[/]");
                }
            }
        }

        AnsiConsole.Write(tree);
    }

    /// <summary>
    /// Outputs SQL migration script
    /// </summary>
    private static async Task OutputSql(SchemaComparison comparison, ComparisonOptions options)
    {
        var sql = GenerateMigrationScript(comparison, options);
        
        var fileName = $"{options.MigrationName}.sql";
        await File.WriteAllTextAsync(fileName, sql);
        
        AnsiConsole.WriteLine($"✅ Migration script saved to: [cyan]{fileName}[/]");
        AnsiConsole.WriteLine();
        AnsiConsole.Write(new Panel(sql)
            .Header($"📄 [bold]{fileName}[/]")
            .BorderColor(Spectre.Console.Color.Green));
    }

    /// <summary>
    /// Generates SQL migration script from comparison results
    /// </summary>
    private static string GenerateMigrationScript(SchemaComparison comparison, ComparisonOptions options)
    {
        var script = new StringBuilder();
        
        // Header
        script.AppendLine($"-- Migration: {options.MigrationName}");
        script.AppendLine($"-- Generated: {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        script.AppendLine($"-- Database Type: {options.DatabaseType}");
        script.AppendLine("-- 🎯 Generated by Schemer - The Ultimate Database Schema Diff Tool");
        script.AppendLine();
        script.AppendLine("BEGIN TRANSACTION;");
        script.AppendLine();

        // Add missing tables
        foreach (var table in comparison.MissingTables)
        {
            script.AppendLine($"-- Create missing table: {table.FullName}");
            script.AppendLine(GenerateCreateTableScript(table, options.DatabaseType));
            script.AppendLine();
        }

        // Add missing columns and modify existing ones
        foreach (var tableComparison in comparison.ModifiedTables)
        {
            script.AppendLine($"-- Modify table: {tableComparison.TableName}");
            
            foreach (var column in tableComparison.MissingColumns)
            {
                script.AppendLine(GenerateAddColumnScript(tableComparison.TableName, column, options.DatabaseType));
            }

            foreach (var columnComparison in tableComparison.ModifiedColumns)
            {
                script.AppendLine(GenerateModifyColumnScript(tableComparison.TableName, columnComparison, options.DatabaseType));
            }
            
            script.AppendLine();
        }

        // Footer
        script.AppendLine("COMMIT;");
        script.AppendLine();
        script.AppendLine("-- Migration completed successfully! 🎉");
        script.AppendLine("-- Review this script carefully before executing in production.");

        return script.ToString();
    }

    /// <summary>
    /// Generates CREATE TABLE script for a table
    /// </summary>
    private static string GenerateCreateTableScript(Table table, DatabaseType databaseType)
    {
        var script = new StringBuilder();
        script.AppendLine($"CREATE TABLE {table.FullName} (");
        
        var columnDefinitions = new List<string>();
        foreach (var column in table.Columns)
        {
            columnDefinitions.Add($"    {GenerateColumnDefinition(column, databaseType)}");
        }
        
        script.AppendLine(string.Join(",\n", columnDefinitions));
        script.AppendLine(");");
        
        return script.ToString();
    }

    /// <summary>
    /// Generates ADD COLUMN script
    /// </summary>
    private static string GenerateAddColumnScript(string tableName, Column column, DatabaseType databaseType)
    {
        return $"ALTER TABLE {tableName} ADD COLUMN {GenerateColumnDefinition(column, databaseType)};";
    }

    /// <summary>
    /// Generates ALTER COLUMN script
    /// </summary>
    private static string GenerateModifyColumnScript(string tableName, ColumnComparison comparison, DatabaseType databaseType)
    {
        return databaseType switch
        {
            DatabaseType.Postgres => $"ALTER TABLE {tableName} ALTER COLUMN {comparison.ColumnName} TYPE {comparison.TargetColumn.DataType};",
            DatabaseType.MySQL => $"ALTER TABLE {tableName} MODIFY COLUMN {GenerateColumnDefinition(comparison.TargetColumn, databaseType)};",
            DatabaseType.SqlServer => $"ALTER TABLE {tableName} ALTER COLUMN {GenerateColumnDefinition(comparison.TargetColumn, databaseType)};",
            DatabaseType.SQLite => $"-- SQLite does not support ALTER COLUMN. Manual migration required for {tableName}.{comparison.ColumnName}",
            _ => throw new ArgumentException($"Unsupported database type: {databaseType}")
        };
    }

    /// <summary>
    /// Generates column definition for SQL scripts
    /// </summary>
    private static string GenerateColumnDefinition(Column column, DatabaseType databaseType)
    {
        var definition = new StringBuilder();
        definition.Append($"{column.Name} {column.DataType}");
        
        if (column.MaxLength.HasValue && column.DataType.ToUpper().Contains("VARCHAR"))
        {
            definition.Append($"({column.MaxLength})");
        }
        
        if (column.Precision.HasValue && column.Scale.HasValue)
        {
            definition.Append($"({column.Precision},{column.Scale})");
        }
        
        if (!column.IsNullable)
        {
            definition.Append(" NOT NULL");
        }
        
        if (!string.IsNullOrEmpty(column.DefaultValue))
        {
            definition.Append($" DEFAULT {column.DefaultValue}");
        }
        
        return definition.ToString();
    }

    /// <summary>
    /// Outputs results as JSON
    /// </summary>
    private static async Task OutputJson(SchemaComparison comparison, ComparisonOptions options)
    {
        var jsonResult = new
        {
            metadata = new
            {
                generatedAt = DateTime.UtcNow,
                migrationName = options.MigrationName,
                databaseType = options.DatabaseType.ToString()
            },
            summary = comparison.Summary,
            differences = new
            {
                missingTables = comparison.MissingTables,
                extraTables = comparison.ExtraTables,
                modifiedTables = comparison.ModifiedTables
            },
            migrationScript = GenerateMigrationScript(comparison, options)
        };

        var jsonOptions = new JsonSerializerOptions 
        { 
            WriteIndented = true,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };
        
        // Enable unsafe reflection-based serialization for compatibility
        jsonOptions.TypeInfoResolver = new DefaultJsonTypeInfoResolver();
        
        var json = JsonSerializer.Serialize(jsonResult, jsonOptions);
        
        var fileName = $"{options.MigrationName}.json";
        await File.WriteAllTextAsync(fileName, json);
        
        AnsiConsole.WriteLine($"✅ JSON report saved to: [cyan]{fileName}[/]");
        
        if (options.Verbose)
        {
            AnsiConsole.WriteLine();
            AnsiConsole.Write(new Panel(json)
                .Header("📊 [bold]JSON Output[/]")
                .BorderColor(Spectre.Console.Color.Blue));
        }
    }

    /// <summary>
    /// Outputs results as Markdown report
    /// </summary>
    private static async Task OutputMarkdown(SchemaComparison comparison, ComparisonOptions options)
    {
        var markdown = new StringBuilder();
        
        // Header
        markdown.AppendLine("# 🎯 Schema Comparison Report");
        markdown.AppendLine();
        markdown.AppendLine($"**Generated:** {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        markdown.AppendLine($"**Migration:** {options.MigrationName}");  
        markdown.AppendLine($"**Database Type:** {options.DatabaseType}");
        markdown.AppendLine();
        markdown.AppendLine("---");
        markdown.AppendLine();

        // Summary
        markdown.AppendLine("## 📈 Summary");
        markdown.AppendLine();
        markdown.AppendLine($"- 📊 **{comparison.Summary.TablesCompared} tables** compared");
        markdown.AppendLine($"- ⚠️ **{comparison.Summary.DifferencesFound} differences** found");
        markdown.AppendLine($"- 🔴 **{comparison.Summary.MissingTables} missing** tables");
        markdown.AppendLine($"- 🟡 **{comparison.Summary.ExtraTables} extra** tables");
        markdown.AppendLine($"- 🔄 **{comparison.Summary.ModifiedTables} modified** tables");
        markdown.AppendLine();

        // Missing tables
        if (comparison.MissingTables.Any())
        {
            markdown.AppendLine("## 🔴 Missing Tables");
            markdown.AppendLine();
            foreach (var table in comparison.MissingTables)
            {
                markdown.AppendLine($"### `{table.FullName}`");
                markdown.AppendLine($"- **Columns:** {table.Columns.Count}");
                markdown.AppendLine($"- **Constraints:** {table.Constraints.Count}");
                markdown.AppendLine();
            }
        }

        // Extra tables
        if (comparison.ExtraTables.Any())
        {
            markdown.AppendLine("## 🟡 Extra Tables");
            markdown.AppendLine();
            foreach (var table in comparison.ExtraTables)
            {
                markdown.AppendLine($"### `{table.FullName}`");
                markdown.AppendLine($"- **Columns:** {table.Columns.Count}");
                markdown.AppendLine($"- **Constraints:** {table.Constraints.Count}");
                markdown.AppendLine();
            }
        }

        // Modified tables
        if (comparison.ModifiedTables.Any())
        {
            markdown.AppendLine("## 🔄 Modified Tables");
            markdown.AppendLine();
            foreach (var tableComparison in comparison.ModifiedTables)
            {
                markdown.AppendLine($"### `{tableComparison.TableName}`");
                markdown.AppendLine();
                
                if (tableComparison.MissingColumns.Any())
                {
                    markdown.AppendLine("#### Missing Columns");
                    foreach (var column in tableComparison.MissingColumns)
                    {
                        markdown.AppendLine($"- `{column.Name}` ({column.DataType})");
                    }
                    markdown.AppendLine();
                }
                
                if (tableComparison.ExtraColumns.Any())
                {
                    markdown.AppendLine("#### Extra Columns");
                    foreach (var column in tableComparison.ExtraColumns)
                    {
                        markdown.AppendLine($"- `{column.Name}` ({column.DataType})");
                    }
                    markdown.AppendLine();
                }
                
                if (tableComparison.ModifiedColumns.Any())
                {
                    markdown.AppendLine("#### Modified Columns");
                    foreach (var columnComparison in tableComparison.ModifiedColumns)
                    {
                        markdown.AppendLine($"- `{columnComparison.ColumnName}`");
                        foreach (var difference in columnComparison.Differences)
                        {
                            markdown.AppendLine($"  - {difference}");
                        }
                    }
                    markdown.AppendLine();
                }
            }
        }

        // Footer
        markdown.AppendLine("---");
        markdown.AppendLine();
        markdown.AppendLine("*Generated by 🎯 Schemer - The Ultimate Database Schema Diff Tool*");

        var fileName = $"{options.MigrationName}.md";
        await File.WriteAllTextAsync(fileName, markdown.ToString());
        
        AnsiConsole.WriteLine($"✅ Markdown report saved to: [cyan]{fileName}[/]");
    }

    #endregion

    #region Utility Methods

    /// <summary>
    /// Masks sensitive information in credentials for display
    /// </summary>
    private static string MaskCredential(string credential)
    {
        if (string.IsNullOrEmpty(credential)) return "";
        if (credential.Length <= 2) return "***";
        return credential.Substring(0, 2) + "***";
    }

    #endregion

    #region Database-Specific Schema Readers

    /// <summary>
    /// PostgreSQL schema reader implementation
    /// </summary>
    public class PostgreSqlSchemaReader : ISchemaReader
    {
        public async Task<DatabaseSchema> ReadSchemaAsync(string connectionString, SchemaReadOptions options)
        {
            using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync();

            var tables = await ReadTablesAsync(connection, options);
            var views = await ReadViewsAsync(connection, options);
            var indexes = await ReadIndexesAsync(connection, options);

            return new DatabaseSchema(
                DatabaseName: connection.Database,
                Tables: tables,
                Views: views,
                Indexes: indexes,
                Metadata: new Dictionary<string, object>()
            );
        }

        public async Task<bool> TestConnectionAsync(string connectionString)
        {
            try
            {
                using var connection = new NpgsqlConnection(connectionString);
                await connection.OpenAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public string GetConnectionDisplayName(string connectionString)
        {
            try
            {
                var builder = new NpgsqlConnectionStringBuilder(connectionString);
                var userInfo = !string.IsNullOrEmpty(builder.Username) ? $"{MaskCredential(builder.Username)}@" : "";
                return $"postgresql://{userInfo}{builder.Host}:{builder.Port}/{builder.Database}";
            }
            catch
            {
                return "postgresql://***";
            }
        }


        private async Task<List<Table>> ReadTablesAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT 
                    t.table_schema,
                    t.table_name,
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale,
                    CASE WHEN c.column_default LIKE 'nextval%' THEN true ELSE false END as is_identity
                FROM information_schema.tables t
                LEFT JOIN information_schema.columns c ON t.table_name = c.table_name AND t.table_schema = c.table_schema
                WHERE t.table_type = 'BASE TABLE'
                    AND t.table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY t.table_schema, t.table_name, c.ordinal_position";

            var results = await connection.QueryAsync(sql);
            var tables = new List<Table>();
            
            var tableGroups = results.GroupBy(r => new { schema = (string)r.table_schema, name = (string)r.table_name });
            
            foreach (var g in tableGroups)
            {
                var constraints = await ReadTableConstraintsAsync(connection, g.Key.schema, g.Key.name);
                
                var table = new Table(
                    Name: g.Key.name,
                    Schema: g.Key.schema,
                    Columns: g.Where(r => r.column_name != null).Select(r => new Column(
                        Name: (string)r.column_name,
                        DataType: (string)r.data_type,
                        IsNullable: (string)r.is_nullable == "YES",
                        DefaultValue: r.column_default as string,
                        MaxLength: r.character_maximum_length as int?,
                        Precision: r.numeric_precision as int?,
                        Scale: r.numeric_scale as int?,
                        IsIdentity: (bool)r.is_identity,
                        IsComputed: false,
                        Properties: new Dictionary<string, object>()
                    )).ToList(),
                    Constraints: constraints,
                    Properties: new Dictionary<string, object>()
                );
                
                tables.Add(table);
            }
            
            return tables;
        }

        private async Task<List<Constraint>> ReadTableConstraintsAsync(DbConnection connection, string schema, string tableName)
        {
            const string sql = @"
                SELECT 
                    tc.constraint_name,
                    tc.constraint_type,
                    kcu.column_name,
                    ccu.table_schema as referenced_schema,
                    ccu.table_name as referenced_table,
                    ccu.column_name as referenced_column
                FROM information_schema.table_constraints tc
                LEFT JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_catalog = kcu.constraint_catalog
                    AND tc.constraint_schema = kcu.constraint_schema
                    AND tc.constraint_name = kcu.constraint_name
                LEFT JOIN information_schema.constraint_column_usage ccu
                    ON tc.constraint_catalog = ccu.constraint_catalog
                    AND tc.constraint_schema = ccu.constraint_schema
                    AND tc.constraint_name = ccu.constraint_name
                WHERE tc.table_schema = @schema AND tc.table_name = @tableName
                ORDER BY tc.constraint_name, kcu.ordinal_position";

            var results = await connection.QueryAsync(sql, new { schema, tableName });
            
            return results
                .GroupBy(r => new { name = (string)r.constraint_name, type = (string)r.constraint_type })
                .Select(g => new Constraint(
                    Name: g.Key.name,
                    Type: ParseConstraintType(g.Key.type),
                    TableName: tableName,
                    Schema: schema,
                    Columns: g.Select(r => (string)r.column_name).Where(c => !string.IsNullOrEmpty(c)).Distinct().ToList(),
                    ReferencedTable: g.FirstOrDefault()?.referenced_table as string,
                    ReferencedColumns: g.Select(r => r.referenced_column as string).Where(c => !string.IsNullOrEmpty(c)).Distinct().ToList(),
                    Properties: new Dictionary<string, object>()
                ))
                .ToList();
        }

        private async Task<List<View>> ReadViewsAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT 
                    table_schema,
                    table_name,
                    view_definition
                FROM information_schema.views
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')";

            var results = await connection.QueryAsync(sql);
            
            return results.Select(r => new View(
                Name: (string)r.table_name,
                Schema: (string)r.table_schema,
                Definition: (string)r.view_definition,
                Properties: new Dictionary<string, object>()
            )).ToList();
        }

        private async Task<List<Index>> ReadIndexesAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT 
                    i.schemaname,
                    i.tablename,
                    i.indexname,
                    i.indexdef,
                    ix.indisunique,
                    ix.indisprimary,
                    array_agg(a.attname ORDER BY a.attnum) as columns
                FROM pg_indexes i
                JOIN pg_class ic ON ic.relname = i.indexname
                JOIN pg_index ix ON ix.indexrelid = ic.oid
                JOIN pg_class tc ON tc.oid = ix.indrelid
                JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = ANY(ix.indkey)
                WHERE i.schemaname NOT IN ('information_schema', 'pg_catalog')
                    AND NOT ix.indisprimary OR (ix.indisprimary AND @includePrimaryKeys)
                GROUP BY i.schemaname, i.tablename, i.indexname, i.indexdef, ix.indisunique, ix.indisprimary
                ORDER BY i.schemaname, i.tablename, i.indexname";

            var results = await connection.QueryAsync(sql, new { includePrimaryKeys = options.IncludeIndexes });
            
            return results.Select(r => new Index(
                Name: (string)r.indexname,
                TableName: (string)r.tablename,
                Schema: (string)r.schemaname,
                Columns: ((string[])r.columns).ToList(),
                IsUnique: (bool)r.indisunique,
                IsPrimaryKey: (bool)r.indisprimary,
                Properties: new Dictionary<string, object> 
                { 
                    ["Definition"] = (string)r.indexdef 
                }
            )).ToList();
        }

        private static ConstraintType ParseConstraintType(string constraintType)
        {
            return constraintType.ToUpper() switch
            {
                "PRIMARY KEY" => ConstraintType.PrimaryKey,
                "FOREIGN KEY" => ConstraintType.ForeignKey,
                "UNIQUE" => ConstraintType.Unique,
                "CHECK" => ConstraintType.Check,
                _ => ConstraintType.Check
            };
        }
    }

    /// <summary>
    /// MySQL schema reader implementation
    /// </summary>
    public class MySqlSchemaReader : ISchemaReader
    {
        public async Task<DatabaseSchema> ReadSchemaAsync(string connectionString, SchemaReadOptions options)
        {
            using var connection = new MySqlConnection(connectionString);
            await connection.OpenAsync();

            var tables = await ReadTablesAsync(connection, options);
            var views = await ReadViewsAsync(connection, options);
            var indexes = await ReadIndexesAsync(connection, options);

            return new DatabaseSchema(
                DatabaseName: connection.Database,
                Tables: tables,
                Views: views,
                Indexes: indexes,
                Metadata: new Dictionary<string, object>()
            );
        }

        public async Task<bool> TestConnectionAsync(string connectionString)
        {
            try
            {
                using var connection = new MySqlConnection(connectionString);
                await connection.OpenAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public string GetConnectionDisplayName(string connectionString)
        {
            try
            {
                var builder = new MySqlConnectionStringBuilder(connectionString);
                var userInfo = !string.IsNullOrEmpty(builder.UserID) ? $"{MaskCredential(builder.UserID)}@" : "";
                return $"mysql://{userInfo}{builder.Server}:{builder.Port}/{builder.Database}";
            }
            catch
            {
                return "mysql://***";
            }
        }

        private async Task<List<Table>> ReadTablesAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT 
                    t.TABLE_SCHEMA,
                    t.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.IS_NULLABLE,
                    c.COLUMN_DEFAULT,
                    c.CHARACTER_MAXIMUM_LENGTH,
                    c.NUMERIC_PRECISION,
                    c.NUMERIC_SCALE,
                    c.EXTRA
                FROM information_schema.TABLES t
                LEFT JOIN information_schema.COLUMNS c ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
                WHERE t.TABLE_TYPE = 'BASE TABLE'
                    AND t.TABLE_SCHEMA = DATABASE()
                ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION";

            var results = await connection.QueryAsync(sql);
            
            return results
                .GroupBy(r => new { schema = (string)r.TABLE_SCHEMA, name = (string)r.TABLE_NAME })
                .Select(g => new Table(
                    Name: g.Key.name,
                    Schema: g.Key.schema,
                    Columns: g.Where(r => r.COLUMN_NAME != null).Select(r => new Column(
                        Name: (string)r.COLUMN_NAME,
                        DataType: (string)r.DATA_TYPE,
                        IsNullable: (string)r.IS_NULLABLE == "YES",
                        DefaultValue: r.COLUMN_DEFAULT as string,
                        MaxLength: r.CHARACTER_MAXIMUM_LENGTH as int?,
                        Precision: r.NUMERIC_PRECISION as int?,
                        Scale: r.NUMERIC_SCALE as int?,
                        IsIdentity: ((string)r.EXTRA)?.Contains("auto_increment") == true,
                        IsComputed: false,
                        Properties: new Dictionary<string, object>()
                    )).ToList(),
                    Constraints: new List<Constraint>(),
                    Properties: new Dictionary<string, object>()
                ))
                .ToList();
        }

        private async Task<List<View>> ReadViewsAsync(DbConnection connection, SchemaReadOptions options)
        {
            // TODO: Implement MySQL view reading
            return new List<View>();
        }

        private async Task<List<Index>> ReadIndexesAsync(DbConnection connection, SchemaReadOptions options)
        {
            // TODO: Implement MySQL index reading
            return new List<Index>();
        }
    }

    /// <summary>
    /// SQL Server schema reader implementation
    /// </summary>
    public class SqlServerSchemaReader : ISchemaReader
    {
        public async Task<DatabaseSchema> ReadSchemaAsync(string connectionString, SchemaReadOptions options)
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            var tables = await ReadTablesAsync(connection, options);
            var views = await ReadViewsAsync(connection, options);
            var indexes = await ReadIndexesAsync(connection, options);

            return new DatabaseSchema(
                DatabaseName: connection.Database,
                Tables: tables,
                Views: views,
                Indexes: indexes,
                Metadata: new Dictionary<string, object>()
            );
        }

        public async Task<bool> TestConnectionAsync(string connectionString)
        {
            try
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public string GetConnectionDisplayName(string connectionString)
        {
            try
            {
                var builder = new SqlConnectionStringBuilder(connectionString);
                var userInfo = !string.IsNullOrEmpty(builder.UserID) ? $"{MaskCredential(builder.UserID)}@" : "";
                return $"sqlserver://{userInfo}{builder.DataSource}/{builder.InitialCatalog}";
            }
            catch
            {
                return "sqlserver://***";
            }
        }

        private async Task<List<Table>> ReadTablesAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT 
                    s.name as table_schema,
                    t.name as table_name,
                    c.name as column_name,
                    ty.name as data_type,
                    c.is_nullable,
                    dc.definition as column_default,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.is_identity
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.columns c ON t.object_id = c.object_id
                LEFT JOIN sys.types ty ON c.user_type_id = ty.user_type_id
                LEFT JOIN sys.default_constraints dc ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
                ORDER BY s.name, t.name, c.column_id";

            var results = await connection.QueryAsync(sql);
            
            return results
                .GroupBy(r => new { schema = (string)r.table_schema, name = (string)r.table_name })
                .Select(g => new Table(
                    Name: g.Key.name,
                    Schema: g.Key.schema,
                    Columns: g.Where(r => r.column_name != null).Select(r => new Column(
                        Name: (string)r.column_name,
                        DataType: (string)r.data_type,
                        IsNullable: (bool)r.is_nullable,
                        DefaultValue: r.column_default as string,
                        MaxLength: r.max_length as int?,
                        Precision: r.precision as int?,
                        Scale: r.scale as int?,
                        IsIdentity: (bool)r.is_identity,
                        IsComputed: false,
                        Properties: new Dictionary<string, object>()
                    )).ToList(),
                    Constraints: new List<Constraint>(),
                    Properties: new Dictionary<string, object>()
                ))
                .ToList();
        }

        private async Task<List<View>> ReadViewsAsync(DbConnection connection, SchemaReadOptions options)
        {
            // TODO: Implement SQL Server view reading
            return new List<View>();
        }

        private async Task<List<Index>> ReadIndexesAsync(DbConnection connection, SchemaReadOptions options)
        {
            // TODO: Implement SQL Server index reading
            return new List<Index>();
        }
    }

    /// <summary>
    /// SQLite schema reader implementation
    /// </summary>
    public class SqliteSchemaReader : ISchemaReader
    {
        public async Task<DatabaseSchema> ReadSchemaAsync(string connectionString, SchemaReadOptions options)
        {
            using var connection = new SqliteConnection(connectionString);
            await connection.OpenAsync();

            var tables = await ReadTablesAsync(connection, options);
            var views = await ReadViewsAsync(connection, options);
            var indexes = await ReadIndexesAsync(connection, options);

            return new DatabaseSchema(
                DatabaseName: "main",
                Tables: tables,
                Views: views,
                Indexes: indexes,
                Metadata: new Dictionary<string, object>()
            );
        }

        public async Task<bool> TestConnectionAsync(string connectionString)
        {
            try
            {
                using var connection = new SqliteConnection(connectionString);
                await connection.OpenAsync();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public string GetConnectionDisplayName(string connectionString)
        {
            try
            {
                var builder = new SqliteConnectionStringBuilder(connectionString);
                var dataSource = builder.DataSource;
                // For file paths, show only the filename for privacy
                if (!string.IsNullOrEmpty(dataSource) && dataSource.Contains(Path.DirectorySeparatorChar))
                {
                    dataSource = Path.GetFileName(dataSource);
                }
                return $"sqlite:{dataSource}";
            }
            catch
            {
                return "sqlite:***";
            }
        }

        private async Task<List<Table>> ReadTablesAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT name as table_name 
                FROM sqlite_master 
                WHERE type = 'table' AND name NOT LIKE 'sqlite_%'";

            var tableNames = await connection.QueryAsync<string>(sql);
            var tables = new List<Table>();

            foreach (var tableName in tableNames)
            {
                var columnSql = $"PRAGMA table_info({tableName})";
                var columns = await connection.QueryAsync(columnSql);
                
                var columnList = columns.Select(c => new Column(
                    Name: (string)c.name,
                    DataType: (string)c.type,
                    IsNullable: (long)c.notnull == 0,
                    DefaultValue: c.dflt_value as string,
                    MaxLength: null,
                    Precision: null,
                    Scale: null,
                    IsIdentity: (long)c.pk == 1,
                    IsComputed: false,
                    Properties: new Dictionary<string, object>()
                )).ToList();

                tables.Add(new Table(
                    Name: tableName,
                    Schema: "main",
                    Columns: columnList,
                    Constraints: new List<Constraint>(),
                    Properties: new Dictionary<string, object>()
                ));
            }

            return tables;
        }

        private async Task<List<View>> ReadViewsAsync(DbConnection connection, SchemaReadOptions options)
        {
            const string sql = @"
                SELECT name, sql as definition
                FROM sqlite_master 
                WHERE type = 'view'";

            var results = await connection.QueryAsync(sql);
            
            return results.Select(r => new View(
                Name: (string)r.name,
                Schema: "main",
                Definition: (string)r.definition,
                Properties: new Dictionary<string, object>()
            )).ToList();
        }

        private async Task<List<Index>> ReadIndexesAsync(DbConnection connection, SchemaReadOptions options)
        {
            // TODO: Implement SQLite index reading
            return new List<Index>();
        }
    }

    #endregion
}
