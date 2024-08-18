from flask import Flask, render_template, request, jsonify
import pymysql

app = Flask(__name__, template_folder='templates2')

def get_db_connection():
    return pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='Pwd123#@!', db='C425', charset='utf8')

def fetch_table_names():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        table_names = [row[0] for row in cursor.fetchall()]
        conn.close()
        return table_names
    except pymysql.Error as e:
        return []

def fetch_column_names(table_name):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f"DESC {table_name}")
        column_names = [row[0] for row in cursor.fetchall()]
        conn.close()
        return column_names
    except pymysql.Error as e:
        return []

def fetch_total_rows(table_name):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        total_rows_table = cursor.fetchone()[0]

        conn.close()

        return total_rows_table

    except pymysql.Error as e:
        return {}

def fetch_column_info(table_name):
    try:
        conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='Pwd123#@!', db='C425', charset='utf8')
        cursor = conn.cursor()
        cursor.execute(f"DESC {table_name}")
        column_names = [row[0] for row in cursor.fetchall()]
        conn.close()

        # Construct SQL SELECT statement
        column_info = {
            'columnInfo': f"SELECT {', '.join(column_names)} FROM {table_name}",
            'columnNames': column_names
        }

        return column_info
    except pymysql.Error as e:
        # Handle pymysql errors
        return {'columnInfo': '', 'columnNames': []}



@app.route('/')
def index():
    table_names = fetch_table_names()
    return render_template('index.html', table_names=table_names)

@app.route('/get_columns')
def get_columns():
    try:
        table_name = request.args.get('table')
        column_names = fetch_column_names(table_name)
        return jsonify(column_names)
    except pymysql.Error as e:
        return jsonify([])

@app.route('/get_total_rows')
def get_total_rows():
    try:
        table1_name = request.args.get('table1')
        table2_name = request.args.get('table2')

        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM {table1_name}")
        total_rows_table1 = cursor.fetchone()[0]

        cursor.execute(f"SELECT COUNT(*) FROM {table2_name}")
        total_rows_table2 = cursor.fetchone()[0]

        conn.close()

        return jsonify({'table1': total_rows_table1, 'table2': total_rows_table2})

    except pymysql.Error as e:
        return jsonify({'table1': 0, 'table2': 0})

@app.route('/get_column_info')
def get_column_info():
    try:
        table_name = request.args.get('table')
        column_info = fetch_column_info(table_name)
        return jsonify(column_info)
    except pymysql.Error as e:
        # Handle pymysql errors
        return jsonify({'columnInfo': '', 'columnNames': []})

# Helper function to execute SQL query and fetch results
def execute_sql_query(sql_query, operation, table_name=None):
    try:
        conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='Pwd123#@!', db='C425', charset='utf8')
        cursor = conn.cursor()

        # Execute the SQL query
        cursor.execute(sql_query)
        conn.commit()

        if operation == 'select' or operation == 'set_operation' or operation == 'set_membership' or operation == 'cte' \
                or operation == 'aggregation' or operation == 'olap':
            cursor.execute(sql_query)

        if operation == 'insert' or operation == 'update' or operation == 'delete':
            cursor.execute(f"select * from {table_name}")

        if operation == 'create':
            cursor.execute(f"desc {table_name}")


        # Fetch result and column names
        columns = [description[0] for description in cursor.description]
        result = cursor.fetchall()


        conn.close()

        return result, columns

    except Exception as e:
        # Handle errors
        return None, None


# New route for handling form submission and executing operations
@app.route('/submit', methods=['POST'])
def submit_form():
    try:
        # Get data from the form submission
        operation = request.form.get('operation')

        if operation == 'select':
            # Execute select operation
            table_name = request.form.get('table_select')
            columns = request.form.get('column_info_select')
            where_condition = request.form.get('where_condition_select')


            # Construct the SQL query
            sql_query = f"SELECT {columns} FROM {table_name}"
            if where_condition:
                sql_query += f" WHERE {where_condition}"

            # Execute the SQL query
            result, columns = execute_sql_query(sql_query, operation, table_name)

            return render_template('result.html', operation_type=operation, operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'insert':
            # Execute insert operation
            table_name = request.form.get('table_insert')
            values_info = request.form.get('values_info_insert')

            # Construct the SQL query for insert operation
            sql_query = f"INSERT INTO {table_name} VALUES ({values_info})"

            print(sql_query)

            # Execute the SQL query
            result, columns = execute_sql_query(sql_query, operation, table_name)

            return render_template('result.html', operation_type=operation, operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'update':
            # Execute update operation
            table_name = request.form.get('table_update')
            reset_condition = request.form.get('reset_condition')
            where_condition = request.form.get('where_condition_update')

            # Construct the SQL query for update operation
            sql_query = f"UPDATE {table_name} SET {reset_condition}"

            if where_condition:
                sql_query += f" WHERE {where_condition}"

            # Execute the SQL query
            result, columns = execute_sql_query(sql_query, operation, table_name)

            return render_template('result.html', operation_type=operation, operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'delete':
            # Execute delete operation
            table_name = request.form.get('table_delete')
            where_condition = request.form.get('where_condition_delete')

            # Construct the SQL query for delete operation
            sql_query = f"DELETE FROM {table_name}"

            if where_condition:
                sql_query += f" WHERE {where_condition}"

            # Execute the SQL query
            result, columns = execute_sql_query(sql_query, operation, table_name)

            return render_template('result.html', operation_type=operation, operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'create':
            # Execute create operation
            table_name = request.form.get('table_name_create')
            column_type_info = request.form.get('column_type_info_create')

            # Construct the SQL query for create operation
            sql_query = f"CREATE TABLE {table_name} ({column_type_info})"

            # Execute the SQL query
            result, columns = execute_sql_query(sql_query, operation, table_name)

            return render_template('result.html', operation_type=operation, operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'set_operation':
            # Execute set operation
            table1_name = request.form.get('table1_selection')
            table2_name = request.form.get('table2_selection')
            table_operation = request.form.get('table_operation')
            column_info_set_operation = request.form.get('column_info_set_operation')
            join_info_set_operation = request.form.get('join_info_set_operation')
            join_info_set_operation2 = request.form.get('join_info_set_operation2')

            # Construct the SQL query for set operation
            if table_operation == 'join' or table_operation == 'left join' or table_operation == 'right join':
                sql_query = f"SELECT {column_info_set_operation} FROM {table1_name} {table_operation} {table2_name} ON {table1_name}.{join_info_set_operation} = {table2_name}.{join_info_set_operation2}"
            else:
                sql_query = f"SELECT {column_info_set_operation} FROM {table1_name} {table_operation} SELECT {column_info_set_operation} FROM {table2_name}"

            print(sql_query)
            # Execute the SQL query
            result, columns = execute_sql_query(sql_query, operation, None)

            return render_template('result.html', operation_type=operation, operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'set_comparison':
            # Execute table comparison
            table1_name = request.form.get('table1_selection_comparison')
            table2_name = request.form.get('table2_selection_comparison')

            # Fetch total rows for each table
            total_rows_table1 = fetch_total_rows(table1_name)
            total_rows_table2 = fetch_total_rows(table2_name)

            return render_template('result.html', operation_type='set_comparison', total_rows_table1=total_rows_table1, total_rows_table2=total_rows_table2, operation=operation)

        elif operation == 'set_membership':
            # Execute set membership operation
            table_column = request.form.get('column_info_membership')
            table_name = request.form.get('table_selection_membership')
            filter_column = request.form.get('column_selection_membership')
            values_info_membership = request.form.get('values_info_membership')

            # Generate SQL query for set membership operation
            sql_query = f"SELECT {table_column} FROM {table_name} WHERE {filter_column} IN ({values_info_membership})"

            # Execute the SQL query and fetch results
            result, columns = execute_sql_query(sql_query, operation, table_name)

            # Render the result template
            return render_template('result.html', operation_type='set_membership', operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'cte':
            # Execute CTE operation
            cte_rename = request.form.get('cte_rename')
            cte_table_selection = request.form.get('cte_table_selection')
            cte_column_selection = request.form.get('cte_column_selection')
            cte_where_condition = request.form.get('cte_where_condition')
            cte_select_table = request.form.get('cte_select_table')
            cte_column_info = request.form.get('cte_column_info')

            # Generate SQL query for CTE operation
            sql_query = f"WITH {cte_rename} AS (SELECT {cte_column_selection} FROM {cte_table_selection}"
            if cte_where_condition:
                sql_query += f" WHERE {cte_where_condition})"
            else:
                sql_query += ")"

            sql_query += f" SELECT {cte_column_info} FROM {cte_select_table} WHERE {cte_column_selection} IN (SELECT {cte_column_selection} FROM {cte_rename})"

            # Execute the SQL query and fetch results
            result, columns = execute_sql_query(sql_query, operation, None)

            # Render the result template
            return render_template('result.html', operation_type='cte', operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'aggregation':
            # Execute aggregation operation
            aggregation_method = request.form.get('aggregation_method')
            table_aggregation = request.form.get('table_aggregation')
            column_info_aggregation = request.form.get('column_info_aggregation')
            aggregation_column = request.form.get('aggregation_column')
            partition_by = request.form.get('partition_by')
            order_by = request.form.get('order_by')
            range_value = request.form.get('range')

            # Generate SQL query for aggregation operation
            sql_query = f"SELECT {aggregation_method}({aggregation_column}) over ("

            if partition_by:
                sql_query += f" PARTITION BY {partition_by}"

            if order_by:
                sql_query += f" ORDER BY {order_by}"

            if range_value:
                sql_query += f" {range_value}"

            sql_query += ")"

            if column_info_aggregation:
                sql_query += f", {column_info_aggregation}"

            sql_query += f" FROM {table_aggregation}"

            sql_query += f" GROUP BY {column_info_aggregation}"

            print(sql_query)

            # Execute the SQL query and fetch results
            result, columns = execute_sql_query(sql_query, operation, None)

            # Render the result template
            return render_template('result.html', operation_type='aggregation', operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        elif operation == 'olap':
            # Execute OLAP operation
            table_olap = request.form.get('table_olap')
            column_info_olap = request.form.get('column_info_olap')
            group_condition_olap = request.form.get('group_condition_olap')

            # Generate SQL query for OLAP operation
            sql_query = f"SELECT {column_info_olap} FROM {table_olap}"

            if group_condition_olap:
                sql_query += f" GROUP BY {group_condition_olap} WITH ROLLUP"

            # Execute the SQL query and fetch results
            result, columns = execute_sql_query(sql_query, operation, None)

            # Render the result template
            return render_template('result.html', operation_type='olap', operation_statement=sql_query, columns=columns, rows=result, operation=operation)

        else:
            return render_template('error.html', error_message='Invalid operation type')

    except Exception as e:
        # Handle errors
        return render_template('error.html', error_message='SQL Syntax Error, Please return and check for it!')


@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', error_message='Page not found'), 404

# Custom error handler for 500 Internal Server Error
@app.errorhandler(500)
def internal_server_error(e):
    return render_template('error.html', error_message='Internal Server Error'), 500





if __name__ == '__main__':
    app.run(debug=True)
