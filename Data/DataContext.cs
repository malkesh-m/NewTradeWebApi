using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json;
using System.IO;
using TradeWeb.Repo;
using TradeWeb.Entity;
namespace TradeWeb.API.Data
{
    /// <summary>
    /// TODO  :This class is use in our application code to interact with the underlying database. It manages the database connection and is used to retrieve and save data in the database.
    /// </summary>
    // TODO : a class that derives from the DbContext class
    public class DataContext : DbContext
    {
        // TODO : Create constructor
        public DataContext()
        {
        }
        // TODO : The DbContextOptions instance carries configuration information such as the connection string, database provider to use etc.
        public DataContext(DbContextOptions<DataContext> options)
            : base(options)
        {
        }

        // TODO : Override OnModelCreating method to seed tables data.
        protected override void OnModelCreating(ModelBuilder builder)
        {
            // Add your customizations after calling base.OnModelCreating(builder);
            base.OnModelCreating(builder);

        }

        // TODO : Configure connection.
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            if (!optionsBuilder.IsConfigured)
            {
                var settings = new DataSettingsManager().LoadSettings();
                var connection = settings.ConnectionStrings.DefaultConnection.ToString();
                optionsBuilder.UseSqlServer(connection);
            }
        }
        // TODO : Read appsetting.json file to make a connection from DB
        public class DataSettingsManager
        {
            private const string _dataSettingsFilePath = "appsettings.json";
            public virtual DataSettings LoadSettings()
            {
                var text = File.ReadAllText(_dataSettingsFilePath);
                if (string.IsNullOrEmpty(text))
                    return new DataSettings();
                //get data settings from the JSON file  
                DataSettings settings = JsonConvert.DeserializeObject<DataSettings>(text);
                return settings;
            }
        }
        public class DataSettings
        {
            public ConnectionStrings ConnectionStrings { get; set; }
        }
        public class ConnectionStrings
        {
            public string DefaultConnection { get; set; }
        }

    }
}
