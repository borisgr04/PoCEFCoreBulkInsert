using BulkOperations_EFCore.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EFCore.BulkExtensions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System.Data;
using System.Reflection;
using System.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace BulkOperations_EFCore.BusinessLogic
{
    public class EmployeeService
    {
        private readonly AppDbContext _appDbContext;
        private DateTime Start;
        private TimeSpan TimeSpan;
        //The "duration" variable contains Execution time when we doing the operations (Insert,Update,Delete)
        public EmployeeService(AppDbContext appDbContext)
        {
            _appDbContext = appDbContext;
        }

        #region Add Bulk Data
        public async Task<TimeSpan> AddDataBulkCopyAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            var times = new List<long>();

            var stop = new Stopwatch();
            stop.Start();
            for (int i = 0; i < 100000; i++)
            {
                var employee = new Employee()
                {
                    Name = "Employee_" + i,
                    Designation = "Designation_" + i,
                    City = "City_" + i,
                    Campo1 = "City_" + i,
                    Campo2 = "City_" + i,
                    Campo3 = "City_" + i,
                    Campo4 = "City_" + i,
                    Campo5 = "City_" + i,
                    Campo6 = "City_" + i,
                    Campo7 = "City_" + i,
                    Campo8 = "City_" + i,
                    Campo9 = "City_" + i,
                    Campo10 = "City_" + i,
                    Campo11 = "City_" + i,
                    Campo12 = "City_" + i,
                    Campo13 = "City_" + i,
                    Campo14 = "City_" + i,
                    Campo15 = "City_" + i,
                    Campo16 = "City_" + i,
                    Campo17 = "City_" + i,
                    Campo18 = "City_" + i,
                    Campo20 = "City_" + i,
                    Campo21 = "City_" + i,
                    Campo22 = "City_" + i,
                    Campo23 = "City_" + i,
                    Campo24 = "City_" + i,
                    Campo25 = "City_" + i,
                    Campo26 = "City_" + i,
                    Campo27 = "City_" + i,
                    Campo28 = "City_" + i,
                    Campo29 = "City_" + i,
                    Campo30 = "City_" + i,
                    Campo31 = "City_" + i,
                    Campo32 = "City_" + i,
                    Campo33 = "City_" + i,
                    Campo34 = "City_" + i
                };
            }
            stop.Stop();
            var time = stop.ElapsedMilliseconds;
            times.Add(time);
            var tableName = "employees";
            stop.Restart();
            var dataTable = ListToDataTable(employees, tableName);
            stop.Stop();
            time = stop.ElapsedMilliseconds;
            times.Add(time);
            stop.Restart();
            using (SqlBulkCopy sqlbc = new SqlBulkCopy(_appDbContext.Database.GetConnectionString()))
            {
                sqlbc.DestinationTableName = "employees";
                sqlbc.ColumnMappings.Add("Name", "Name");
                sqlbc.ColumnMappings.Add("Designation", "Designation");
                sqlbc.ColumnMappings.Add("City", "City");
                for (var i=1; i<35;i++) 
                {
                    sqlbc.ColumnMappings.Add($"Campo{i.ToString()}", $"Campo{i.ToString()}");
                }
                await sqlbc.WriteToServerAsync(dataTable);
            }
            stop.Stop();
            time = stop.ElapsedMilliseconds;
            times.Add(time);

            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }

        public async Task<TimeSpan> AddDataBulkNewCopyAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            var times = new List<long>();

            var stop = new Stopwatch();
            stop.Start();
            for (int i = 0; i < 1000; i++)
            {
                var employee = new Employee()
                {
                    Name = "Employee_" + i,
                    Designation = "Designation_" + i,
                    City = "City_" + i,
                    Campo1 = "City_" + i,
                    Campo2 = "City_" + i,
                    Campo3 = "City_" + i,
                    Campo4 = "City_" + i,
                    Campo5 = "City_" + i,
                    Campo6 = "City_" + i,
                    Campo7 = "City_" + i,
                    Campo8 = "City_" + i,
                    Campo9 = "City_" + i,
                    Campo10 = "City_" + i,
                    Campo11 = "City_" + i,
                    Campo12 = "City_" + i,
                    Campo13 = "City_" + i,
                    Campo14 = "City_" + i,
                    Campo15 = "City_" + i,
                    Campo16 = "City_" + i,
                    Campo17 = "City_" + i,
                    Campo18 = "City_" + i,
                    Campo20 = "City_" + i,
                    Campo21 = "City_" + i,
                    Campo22 = "City_" + i,
                    Campo23 = "City_" + i,
                    Campo24 = "City_" + i,
                    Campo25 = "City_" + i,
                    Campo26 = "City_" + i,
                    Campo27 = "City_" + i,
                    Campo28 = "City_" + i,
                    Campo29 = "City_" + i,
                    Campo30 = "City_" + i,
                    Campo31 = "City_" + i,
                    Campo32 = "City_" + i,
                    Campo33 = "City_" + i,
                    Campo34 = "City_" + i
                };
            }
            stop.Stop();
            var time = stop.ElapsedMilliseconds;
            times.Add(time);
            stop.Restart();
            await AddRangeCopyAsync(employees);
            stop.Stop();
            time = stop.ElapsedMilliseconds;
            times.Add(time);
      

            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }

        public async Task AddRangeCopyAsync<T>(List<T> list) 
        {
            Type temp = typeof(T);
            var dataTable = ListToDataTable(list, temp.Name);
            using (SqlBulkCopy sqlbc = new SqlBulkCopy(_appDbContext.Database.GetConnectionString()))
            {
                sqlbc.DestinationTableName = temp.Name;

                foreach (PropertyInfo info in typeof(T).GetProperties())
                {
                    sqlbc.ColumnMappings.Add(info.Name, info.Name);
                }

                await sqlbc.WriteToServerAsync(dataTable);
            }
        }

        public static DataTable ListToDataTable<T>(List<T> list, string _tableName)
        {
            DataTable dt = new DataTable(_tableName);

            foreach (PropertyInfo info in typeof(T).GetProperties())
            {
                dt.Columns.Add(new DataColumn(info.Name, Nullable.GetUnderlyingType(info.PropertyType) ?? info.PropertyType));
            }
            foreach (T t in list)
            {
                DataRow row = dt.NewRow();
                foreach (PropertyInfo info in typeof(T).GetProperties())
                {
                    row[info.Name] = info.GetValue(t, null) ?? DBNull.Value;
                }
                dt.Rows.Add(row);
            }
            return dt;
        }
        public async Task<TimeSpan> AddDataAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            for (int i = 0; i < 100000; i++)
            {
                var employee=new Employee()
                {
                    Name = "Employee_" + i,
                    Designation = "Designation_" + i,
                    City = "City_" + i,
                    Campo1 = "City_" + i,
                    Campo2 = "City_" + i,
                    Campo3 = "City_" + i,
                    Campo4 = "City_" + i,
                    Campo5 = "City_" + i,
                    Campo6 = "City_" + i,
                    Campo7 = "City_" + i,
                    Campo8 = "City_" + i,
                    Campo9 = "City_" + i,
                    Campo10 = "City_" + i,
                    Campo11 = "City_" + i,
                    Campo12 = "City_" + i,
                    Campo13 = "City_" + i,
                    Campo14 = "City_" + i,
                    Campo15 = "City_" + i,
                    Campo16 = "City_" + i,
                    Campo17 = "City_" + i,
                    Campo18 = "City_" + i,
                    Campo20 = "City_" + i,
                    Campo21 = "City_" + i,
                    Campo22 = "City_" + i,
                    Campo23 = "City_" + i,
                    Campo24 = "City_" + i,
                    Campo25 = "City_" + i,
                    Campo26 = "City_" + i,
                    Campo27 = "City_" + i,
                    Campo28 = "City_" + i,
                    Campo29 = "City_" + i,
                    Campo30 = "City_" + i,
                    Campo31 = "City_" + i,
                    Campo32 = "City_" + i,
                    Campo33 = "City_" + i,
                    Campo34 = "City_" + i
                };

                _appDbContext.Employees.Add(employee);
            }
            await _appDbContext.SaveChangesAsync();
            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }

        public async Task<TimeSpan> AddRangeDataAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            for (int i = 0; i < 100000; i++)
            {
                employees.Add(new Employee()
                {
                    Name = "Employee_" + i,
                    Designation = "Designation_" + i,
                    City = "City_" + i,
                    Campo1 = "City_" + i,
                    Campo2 = "City_" + i,
                    Campo3 = "City_" + i,
                    Campo4 = "City_" + i,
                    Campo5 = "City_" + i,
                    Campo6 = "City_" + i,
                    Campo7 = "City_" + i,
                    Campo8 = "City_" + i,
                    Campo9 = "City_" + i,
                    Campo10 = "City_" + i,
                    Campo11 = "City_" + i,
                    Campo12 = "City_" + i,
                    Campo13 = "City_" + i,
                    Campo14 = "City_" + i,
                    Campo15 = "City_" + i,
                    Campo16 = "City_" + i,
                    Campo17 = "City_" + i,
                    Campo18 = "City_" + i,
                    Campo20 = "City_" + i,
                    Campo21 = "City_" + i,
                    Campo22 = "City_" + i,
                    Campo23 = "City_" + i,
                    Campo24 = "City_" + i,
                    Campo25 = "City_" + i,
                    Campo26 = "City_" + i,
                    Campo27 = "City_" + i,
                    Campo28 = "City_" + i,
                    Campo29 = "City_" + i,
                    Campo30 = "City_" + i,
                    Campo31 = "City_" + i,
                    Campo32 = "City_" + i,
                    Campo33 = "City_" + i,
                    Campo34 = "City_" + i
                });
            }
            var stop = new Stopwatch();
            stop.Start();
            _appDbContext.Employees.AddRange(employees);
            await _appDbContext.SaveChangesAsync();
            stop.Stop();
            var time = stop.ElapsedMilliseconds;
            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }

        public async Task<TimeSpan> AddBulkDataAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            for (int i = 0; i < 100000; i++)
            {
                employees.Add(new Employee()
                {
                    Name = "Employee_" + i,
                    Designation = "Designation_" + i,
                    City = "City_" + i
                });
            }
            await _appDbContext.BulkInsertAsync(employees);
            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }
        #endregion

        #region Update Bulk Data
        public async Task<TimeSpan> UpdateBulkDataAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            for (int i = 0; i < 100000; i++)
            {
                employees.Add(new Employee()
                {
                    Id = (i + 1),
                    Name = "Update Employee_" + i,
                    Designation = "Update Designation_" + i,
                    City = "Update City_" + i
                });
            }
            await _appDbContext.BulkUpdateAsync(employees);
            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }
        #endregion

        #region Delete Bulk Data
        public async Task<TimeSpan> DeleteBulkDataAsync()
        {
            List<Employee> employees = new(); // C# 9 Syntax.
            Start = DateTime.Now;
            employees = _appDbContext.Employees.ToList();
            await _appDbContext.BulkDeleteAsync(employees);
            TimeSpan = DateTime.Now - Start;
            return TimeSpan;
        }
        #endregion
    }
}
