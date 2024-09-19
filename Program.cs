using System.Xml.Linq;
using Npgsql;
using System.Globalization;

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
        using (NpgsqlConnection connection = new NpgsqlConnection(connectionString)){
            connection.Open();
            foreach(var order in orders_linq){
                //Console.WriteLine(order.sum);
                
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
        }
    }
    static void InsertDB(NpgsqlConnection con, string table, string[] colNames, object[] columns){
        NpgsqlCommand command = new NpgsqlCommand();
        string strColNames = string.Join(", ", colNames);
        string[] ParColNames = colNames.Select(p => "@" + p).ToArray();
        string strSepColNames = string.Join(", ", ParColNames);
        string[] andColNames = colNames.Select(p => p + " = @" + p).ToArray();
        string strAndColNames = string.Join(" and ", andColNames);
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
}