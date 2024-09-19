using System.Text;
using System.Xml.Linq;
using Npgsql;
using System.Globalization;
using System.Runtime.ConstrainedExecution;

class Program{

    static async Task Main(string[] args){
        XDocument xml = XDocument.Load("data.xml");
        var orders_linq = xml.Element("orders")?
            .Elements("order")
            .Select(p => new {
                no = int.Parse(p.Element("no")?.Value),
                reg_date = p.Element("reg_date")?.Value,
                sum = double.Parse(p.Element("sum")?.Value, CultureInfo.InvariantCulture.NumberFormat),
                products = p.Elements("product")
                    .Select(n => new {
                        quantity = int.Parse(n.Element("quantity")?.Value),
                        name = n.Element("name")?.Value,
                        price = double.Parse(n.Element("price")?.Value, CultureInfo.InvariantCulture)
                    }),
                user =new {
                    fio = p.Element("user")?.Element("fio")?.Value,
                    email = p.Element("user")?.Element("email")?.Value}
            });
        string connectionString = "Host=localhost;Port=5432;Database=istore;User Id=postgres;Password=1488;";
        NpgsqlConnection connection = new NpgsqlConnection(connectionString);
        connection.Open();
        foreach(var order in orders_linq){
            InsertDB(connection, "users", ["fio", "email"], [order.user.fio, order.user.email]);
            DateOnly regDate = DateOnly.FromDateTime(DateTime.ParseExact(order.reg_date, "yyyy.MM.dd", CultureInfo.InvariantCulture));
            int userId = GetFirstId(connection, "users", "email", order.user.email);
            Console.WriteLine(userId);
            
            InsertDB(connection, "orders", ["id", "user_id", "total_amount", "date"], [order.no, userId, order.sum, regDate]);

            foreach(var product in order.products){
                InsertDB(connection, "products", ["name", "price"], [product.name, product.price]);
                int productId = GetFirstId(connection, "products", "name", product.name);
                InsertDB(connection, "order_element", ["product_id", "order_id", "quantity"], [productId, order.no, product.quantity]);
            }

        }
        connection.Close();
    }
    static void InsertDB(NpgsqlConnection con, string table, string[] colNames, object[] columns){
        NpgsqlCommand command = new NpgsqlCommand();
        string strColNames = string.Join(", ", colNames);
        string[] ParColNames = colNames.Select(p => "@" + p).ToArray();
        string strSepColNames = string.Join(", ", ParColNames);
        string[] andColNames = colNames.Select(p => p + " = @" + p).ToArray();
        string strAndColNames = string.Join(" and ", andColNames);
        /*string prepareToCommand = 
              "START TRANSACTION; "
            +$"IF NOT EXISTS (SELECT * FROM {table} WHERE {strAndColNames}) THEN "
            + "BEGIN "
            +   $"INSERT INTO {table} ({strColNames}) VALUES ({strSepColNames}); "
            + "END; "
            + "COMMIT TRANSACTION;";
            */
        string prepareToCommand = 
            $"insert into {table} ({strColNames}) "
            + $"select {strSepColNames} "
            + "where not exists ("
            + $"select 1 from {table} "
            + $"where {strAndColNames});";
        command.CommandText = string.Format(prepareToCommand);
        var zipCol = ParColNames.Zip(columns);
        for (int i = 0; i < colNames.Length; i++){
            command.Parameters.AddWithValue(colNames[i].ToString(), columns[i]);
        }
        command.Connection = con;
        command.ExecuteScalar();
    }

    static void UpdateUsers(string? fio, string? email, NpgsqlConnection con){
        NpgsqlCommand command = new NpgsqlCommand();
        command.CommandText = string.Format(
            "insert into users (fio, email) "
            + "select @fio, @email "
            + "where not exists ("
            + "select 1 from users "
            + "where fio = @fio and email = @email);"
        );
        command.Parameters.AddWithValue("@fio", fio);
        command.Parameters.AddWithValue("@email", email);
        command.Connection = con;
        command.ExecuteNonQuery();
    }

    static int GetFirstId(NpgsqlConnection con, string table, string column, string value){
        NpgsqlCommand command = new NpgsqlCommand();
        command.Connection = con;
        command.CommandText = $"select id from {table} where {column} = '{value}';";
        //command.CommandText = string.Format("select id from @table where @column = '@value';");
        //command.Parameters.AddWithValue("@table", table);
        //command.Parameters.AddWithValue("@column", column);
        //command.Parameters.AddWithValue("@value", value);
        return Convert.ToInt32(command.ExecuteScalar());
    }

    static void UpdateOrders(int id, object user_id, float total_amount, DateOnly date, NpgsqlConnection con){
        NpgsqlCommand command = new NpgsqlCommand();
        command.CommandText = string.Format(
            "insert into orders (id, user_id, total_amount, date) "
            + "select @id,  @user_id, @total_amount, @date "
            + "where not exists ("
            + "select 1 from orders "
            + "where id = @id and user_id = @user_id and total_amount = @total_amount and date = @date);"
        );
        command.Parameters.AddWithValue("@id", id);
        command.Parameters.AddWithValue("@user_id", user_id);
        command.Parameters.AddWithValue("@total_amount", total_amount);
        command.Parameters.AddWithValue("@date", date);
        command.Connection = con;
        command.ExecuteNonQuery();
    }

    static void UpdateProducts(string name, string description, string price, string quantity, NpgsqlConnection con){
        NpgsqlCommand command = new NpgsqlCommand();
        command.CommandText = string.Format(
            "insert into products (name, descriprion, price, quantity) "
            + "select @name, @description, @price, @quantity "
            + "where not exists ("
            + "select 1 from users "
            + "where name = @name and descriprion = @description and price = @price and quantity = @quantity);"
        );
        command.Parameters.AddWithValue("@name", name);
        command.Parameters.AddWithValue("@description", description);
        command.Parameters.AddWithValue("@price", price);
        command.Parameters.AddWithValue("@quantity", quantity);
        command.Connection = con;
        command.ExecuteNonQuery();
    }
}