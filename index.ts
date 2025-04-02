import {
  DynamicModule,
  Global,
  Inject,
  Module,
  ModuleMetadata,
  OnApplicationShutdown,
} from '@nestjs/common';
import {
  Table as _Table,
  BigInt,
  Bit,
  config,
  ConnectionPool,
  IBulkResult,
  Int,
  ISqlType,
  NVarChar,
  PreparedStatement,
  Request,
} from 'mssql';
import { escape } from 'node:querystring';

const tableMetadataKey = Symbol('table');
const columnMetadataKey = Symbol('column');
const innerTable = Symbol('innerTable');
const innerColumns = Symbol('innerColumns');

const MSSQL_MODULE_OPTIONS = Symbol('MSSQL_MODULE_OPTIONS');
const mssqlExtends = Symbol('mssqlExtends');

export function Table(tableName?: string) {
  return function (constructor: object['constructor']) {
    Reflect.defineMetadata(
      tableMetadataKey,
      tableName?.toLowerCase() || constructor.name?.toLowerCase(),
      constructor,
    );
  };
}

type ColumnMetadata = {
  column: string;
  columnName: string;
  type: (() => ISqlType) | ISqlType;
};

type TypeofReturnValue =
  | 'bigint'
  | 'boolean'
  | 'function'
  | 'number'
  | 'object'
  | 'string'
  | 'symbol'
  | 'undefined';

const sqlTypes: Record<TypeofReturnValue, ISqlType | (() => ISqlType)> = {
  bigint: () => BigInt(),
  boolean: () => Bit(),
  number: () => Int(),
  string: () => NVarChar(128),
  function: () => NVarChar(128),
  object: () => NVarChar(128),
  symbol: () => NVarChar(128),
  undefined: () => NVarChar(128),
};

export function Column(options?: {
  name?: string;
  type?: (() => ISqlType) | ISqlType;
}) {
  return function (target: object, propertyKey: string) {
    const columnName = options?.name || propertyKey?.toLowerCase();

    const type =
      options?.type ||
      sqlTypes[
        typeof Reflect.getMetadata('design:type', target, propertyKey)?.()
      ];

    const columns: ColumnMetadata[] =
      Reflect.getMetadata(columnMetadataKey, target.constructor) || [];

    columns.push({
      column: propertyKey,
      columnName,
      type,
    });

    Reflect.defineMetadata(columnMetadataKey, columns, target.constructor);
  };
}

type TableType<T> = {
  [innerTable]: _Table;
  [innerColumns]: string[];
} & T;

type MssqlResult<T> = { exec: () => Promise<T> };

type MssqlConnection = { useDb: (name: string) => Promise<void> };

type MssqlRefTable = { drop: () => Promise<void> };

export type MssqlTable<T = unknown> = Omit<
  _MssqlTable<T>,
  'onApplicationShutdown'
>;

class _MssqlTable<T = unknown> implements OnApplicationShutdown {
  private readonly [innerTable]: _Table;
  private readonly [innerColumns]: string[];

  private static pool: ConnectionPool;

  public constructor(
    @Inject(MSSQL_MODULE_OPTIONS)
    private readonly options?: MssqlModuleFactoryOptions | symbol,
  ) {
    if (
      !_MssqlTable.pool &&
      options !== mssqlExtends &&
      typeof options !== 'symbol'
    ) {
      if (!options?.config) {
        throw new Error('MSSQL_MODULE_OPTIONS.config is required');
      }

      const appPool = new ConnectionPool(options.config);

      appPool
        .connect()
        .then((pool) => (_MssqlTable.pool = pool))
        .catch((err) => console.error('Error creating connection pool', err));
    }
  }

  public async onApplicationShutdown(): Promise<void> {
    await _MssqlTable.pool?.close();
  }

  private preparedStatement(): PreparedStatement {
    if (!_MssqlTable.pool) {
      throw new Error('Pool not initialized');
    }

    return new PreparedStatement(_MssqlTable.pool);
  }

  private request(): Request {
    if (!_MssqlTable.pool) {
      throw new Error('Pool not initialized');
    }

    return _MssqlTable.pool.request();
  }

  public get db(): MssqlConnection {
    return {
      useDb: async (name: string): Promise<void> => {
        const forbiddenChars = /[^a-zA-Z0-9_]/;
        if (forbiddenChars.test(name)) {
          throw new Error(`Invalid database name '${name}'`);
        }

        const request = this.request();
        await request.query(`USE ${escape(name)}`);
      },
    };
  }

  public get table(): MssqlRefTable {
    return {
      drop: async (): Promise<void> => {
        const request = this.request();
        const query =
          "DECLARE @sql NVARCHAR(MAX); SET @sql = 'DROP TABLE ' + QUOTENAME(@name); EXEC sp_executesql @sql;";
        request.input('name', NVarChar(128), this[innerTable].name);
        await request.query(query);
      },
    };
  }

  public insert(rows: T | T[]): MssqlResult<IBulkResult> {
    if (
      !rows ||
      (Array.isArray(rows) && !rows?.length) ||
      !this[innerColumns]?.length
    ) {
      return;
    }

    const irows = [rows].flat();
    for (const row of irows) {
      const values = this[innerColumns].map((column) => row[column]);
      this[innerTable].rows.add(...values);
    }

    const request = this.request();
    return {
      exec: async () => {
        return await request.bulk(this[innerTable]);
      },
    };
  }
}

export class TableFactory {
  public static createForClass<T>(table: new () => T): MssqlTable<T> {
    const mssqlTable = new _MssqlTable<T>(mssqlExtends);
    const iTable = new table() as TableType<T>;

    const tableName: string = Reflect.getMetadata(tableMetadataKey, table);
    const columns: ColumnMetadata[] = Reflect.getMetadata(
      columnMetadataKey,
      table,
    );

    iTable[innerColumns] = columns?.map((column) => column.column) || [];

    iTable[innerTable] = new _Table(tableName);
    iTable[innerTable].create = true;

    for (const column of columns) {
      iTable[innerTable].columns.add(column.columnName, column.type, {
        nullable: true,
      });
    }

    const proto = Object.getPrototypeOf(mssqlTable);
    const descriptors = Object.getOwnPropertyDescriptors(proto);
    delete descriptors.constructor;
    Object.defineProperties(iTable, descriptors);

    return iTable as unknown as MssqlTable<T>;
  }
}

function getMssqlTable(tableName: string): string {
  return `MSSQL_ORM_TABLE_${tableName.toUpperCase()}`;
}

export function InjectTable(tableName: string) {
  return Inject(getMssqlTable(tableName));
}

export interface TableDefinition {
  name: string;
  table: MssqlTable;
}

export interface MssqlModuleFactoryOptions {
  config: config;
}

export interface MssqlModuleAsyncOptions
  extends Pick<ModuleMetadata, 'imports'> {
  useFactory?: (
    ...args: any[]
  ) => Promise<MssqlModuleFactoryOptions> | MssqlModuleFactoryOptions;
  inject?: any[];
}

@Global()
@Module({ providers: [_MssqlTable] })
export class MssqlModule {
  public static forRootAsync(options: MssqlModuleAsyncOptions): DynamicModule {
    return {
      module: MssqlModule,
      global: true,
      providers: [
        {
          provide: MSSQL_MODULE_OPTIONS,
          useFactory: options.useFactory,
          inject: options.inject || [],
        },
      ],
      exports: [MSSQL_MODULE_OPTIONS],
    };
  }

  public static forFeature(options: TableDefinition): DynamicModule {
    const provider = getMssqlTable(options.name);

    return {
      module: MssqlModule,
      providers: [
        {
          provide: provider,
          useValue: options.table,
        },
      ],
      exports: [provider],
    };
  }
}
