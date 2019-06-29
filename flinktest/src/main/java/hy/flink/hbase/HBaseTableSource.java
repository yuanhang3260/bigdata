package hy.flink.hbase;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.sources.BatchTableSource;
import org.apache.flink.table.sources.ProjectableTableSource;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class HBaseTableSource implements BatchTableSource<Row>, ProjectableTableSource<Row> {

  private Configuration conf;
  private String tableName;
  private HBaseTableSchema hBaseSchema;
  private TableSchema tableSchema;

  /**
   * The HBase configuration and the name of the table to read.
   *
   * @param conf      hbase configuration
   * @param tableName the tableName
   */
  public HBaseTableSource(Configuration conf, String tableName) {
    this.conf = conf;
    this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
    this.hBaseSchema = new HBaseTableSchema();
  }

  private HBaseTableSource(Configuration conf, String tableName, TableSchema tableSchema) {
    this.conf = conf;
    this.tableName = Preconditions.checkNotNull(tableName, "Table  name");
    this.hBaseSchema = new HBaseTableSchema();
    this.tableSchema = tableSchema;
  }

  /**
   * Adds a column defined by family, qualifier, and type to the table schema.
   *
   * @param family    the family name
   * @param qualifier the qualifier name
   * @param clazz     the data type of the qualifier
   */
  public void addColumn(String family, String qualifier, Class<?> clazz) {
    this.hBaseSchema.addColumn(family, qualifier, clazz);
  }

  /**
   * Specifies the charset to parse Strings to HBase byte[] keys and String values.
   *
   * @param charset Name of the charset to use.
   */
  public void setCharset(String charset) {
    this.hBaseSchema.setCharset(charset);
  }

  @Override
  public TypeInformation<Row> getReturnType() {
    return new RowTypeInfo(getFieldTypes(), getFieldNames());
  }

  @Override
  public TableSchema getTableSchema() {
    if (this.tableSchema == null) {
      return new TableSchema(getFieldNames(), getFieldTypes());
    } else {
      return this.tableSchema;
    }
  }

  private String[] getFieldNames() {
    String[] families = hBaseSchema.getFamilyNames();
    String[] fieldNames = new String[families.length + 1];
    fieldNames[0] = "row_key";
    System.arraycopy(families, 0, fieldNames, 1, families.length);
    return fieldNames;
  }

  private TypeInformation[] getFieldTypes() {
    String[] famNames = hBaseSchema.getFamilyNames();
    TypeInformation<?>[] fieldTypes = new TypeInformation[hBaseSchema.getFamilyNames().length + 1];
    fieldTypes[0] = Types.STRING();

    int i = 1;
    for (String family : famNames) {
      fieldTypes[i] = new RowTypeInfo(hBaseSchema.getQualifierTypes(family), hBaseSchema.getQualifierNames(family));
      i++;
    }
    return fieldTypes;
  }

  @Override
  public DataSet<Row> getDataSet(ExecutionEnvironment execEnv) {
    return execEnv.createInput(new HBaseRowInputFormat(conf, tableName, hBaseSchema), getReturnType()).name(explainSource());
  }

  @Override
  public HBaseTableSource projectFields(int[] fields) {
    String[] famNames = hBaseSchema.getFamilyNames();
    HBaseTableSource newTableSource = new HBaseTableSource(this.conf, tableName, getTableSchema().copy());
    // Extract the family from the given fields
    for (int field : fields) {
      if (field == 0) {
        // Skip row_key.
        continue;
      }
      String family = famNames[field - 1];
      Map<String, TypeInformation<?>> familyInfo = hBaseSchema.getFamilyInfo(family);
      for (Map.Entry<String, TypeInformation<?>> entry : familyInfo.entrySet()) {
        // create the newSchema
        String qualifier = entry.getKey();
        newTableSource.addColumn(family, qualifier, entry.getValue().getTypeClass());
      }
    }
    return newTableSource;
  }

  @Override
  public String explainSource() {
    return TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames());
  }
}