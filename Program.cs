using System.Xml.Linq;
using Npgsql;

class XmlOrder{

}

class Program{

    static async Task Main(string[] args){
        XDocument xml = XDocument.Load("data.xml");
        var orders_linq = xml.Element("orders")?
            .Elements("order")
            .Select(p => new {
                no = p.Element("no")?.Value,
                reg_date = p.Element("reg_date")?.Value,
                sum = p.Element("sum")?.Value,
                products = p.Elements("product")
                    .Select(n => new {
                        quantity = n.Element("quantity")?.Value,
                        name = n.Element("name")?.Value,
                        price = n.Element("price")?.Value
                    }),
                user =new {
                    fio = p.Element("user")?.Element("fio")?.Value,
                    email = p.Element("user")?.Element("email")?.Value}
            });
        string connectionString = "Host=localhost;Port=5432;Database=istore;User Id=postgres;Password=1488;";
        using (NpgsqlConnection connection = new NpgsqlConnection(connectionString)){
            await connection.OpenAsync();
            foreach(var order in orders_linq){
                string updateUsers = $" = insert into users (fio, email) select {order.user.fio}, {order.user.email}"+
                    $" where not exists (select 1 from users where fio = {order.user.fio} and email = {order.user.email});";
                    Console.WriteLine(updateUsers);
                NpgsqlCommand command1 = new NpgsqlCommand(updateUsers, connection);
            }
            NpgsqlCommand command = new NpgsqlCommand("select * from users;", connection);
        }
    }


    static void UpdateProducts(XElement? product){
        XElement? quantity = product?.Element("quantity");
        XElement? name = product?.Element("name");
        XElement? price = product?.Element("price");

        Console.WriteLine("Table products was updated");
    }

    static void UpdateOrders(){
        Console.WriteLine("Table orders was updated");
    }

    static void UpdateOrderElement(){
        Console.WriteLine("Table order_element was updated");
    }
}