using System.Xml.Linq;
using Npgsql;
using System.Globalization;

//programm connecting to the postgresql local server
class Program{

    static async Task Main(string[] args){
        // Opening the xml document
        XDocument xml = XDocument.Load("data.xml");
        // configure connection string to connect the db
        string connectionString = "Host=localhost;Port=5432;Database=istore;User Id=postgres;Password=1488;";
        using (NpgsqlConnection connection = new NpgsqlConnection(connectionString)){
            // async opening connection to the db
            await connection.OpenAsync();
            // Reading element by name "orders"
            XElement? orders = xml.Element("orders");
            // if element with name "orders" was find
            if (orders is not null){
                // for each element with name "order" that contains in element "orders"
                foreach(var order in orders.Elements("order")){
                    // reading elements: "no", "reg_date" and "sum"
                    int? no = Convert.ToInt32(order.Element("no")?.Value, CultureInfo.InvariantCulture);
                    string? regDateStr = order.Element("reg_date")?.Value;
                    double? totalAmount = Convert.ToDouble(order.Element("sum")?.Value, CultureInfo.InvariantCulture);

                    // if regDate was find then formatting it to "yyyy-MM-dd"
                    DateOnly? regDate;
                    if (regDateStr is not null){
                        regDate = DateOnly.FromDateTime(DateTime.ParseExact(regDateStr, "yyyy.MM.dd", CultureInfo.InvariantCulture));
                    }
                    else{ regDate = null;}

                    // find Element "user" and his elements: "fio", "email"
                    XElement? user = order.Element("user");
                    string? fio = user?.Element("fio")?.Value;
                    string? email = user?.Element("email")?.Value;
                    int? userId = null;

                    // if elements "fio" and "email" was find inserting this data to the table users
                    if(fio is not null && email is not null){
                        InsertDB(connection, "users", ["fio", "email"], [fio, email]);
                        userId = GetFirstId(connection, "users", "email", email);
                    }

                    // if user was inserted in table users and date of order is consisting then inserting order info in table orders
                    if(userId is not null && regDate is not null){
                        Console.WriteLine($"{no}, {userId}, {totalAmount}, {regDate}");
                        InsertDB(connection, "orders", ["id", "user_id", "total_amount", "date"], [no, userId, totalAmount, regDate]);
                    }

                    // finding elements with name "product" 
                    var products = order.Elements("product");
                    foreach(var product in products){
                        // finding elements: quntity, name and price
                        int? quantity = Convert.ToInt32(product.Element("quantity")?.Value);
                        string? name = product.Element("name")?.Value;
                        double? price = Convert.ToDouble(product.Element("price")?.Value, CultureInfo.InvariantCulture);
                        // if element name was find then inserting in table "products" product and in table "order_element" element of order
                        if(name is not null){
                            InsertDB(connection, "products", ["name", "price"], [name, price]);
                            int productId = GetFirstId(connection, "products", "name", name);
                            InsertDB(connection, "order_element", ["product_id", "order_id", "quantity"], [productId, no, quantity]);
                        }
                    }
                }
            }
        // LINQ TO XML 
        // THE SAME RESULT
        /*
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
            }*/
        }
    }
    /*
    this method takes as input of the connection object, the name of the table, the names of the columns and the values ​​for these columns
    generates the request and sends a query to the database: checks the contents of the row in the table that we want to insert, if there is no such row, then inserts it
    */
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
    //this method takes as input the connection object, the name of the table, the name of the column and the value ​​for this column
    //generate request and sends a query to the database: finds the fisrt index by the one value from input and return it
    static int GetFirstId(NpgsqlConnection con, string table, string column, string value){
        NpgsqlCommand command = new NpgsqlCommand();
        command.Connection = con;
        command.CommandText = $"select id from {table} where {column} = '{value}';";
        // Не работает когда ищешь по почте из-за знака '@'
        /*
        command.CommandText = string.Format("select id from @table where @column = '@value';");
        command.Parameters.AddWithValue("@table", table);
        command.Parameters.AddWithValue("@column", column);
        command.Parameters.AddWithValue("@value", value);*/
        return Convert.ToInt32(command.ExecuteScalar());
    }
}