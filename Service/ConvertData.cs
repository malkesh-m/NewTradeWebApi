using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;

namespace TradeWeb.Service
{
    /// <summary>
    /// TODO : Create a common class for convert DataTable to Entity List.
    /// </summary>
    public class ConvertData
    {
        #region Methods
        public static List<T> ConvertDataTable<T>(DataTable dt)
        {
            List<T> data = new List<T>();
            foreach (DataRow row in dt.Rows)
            {
                T item = GetItem<T>(row);
                data.Add(item);
            }
            return data;
        }

        //public static string ConvertDataTableToJson<T>(DataTable dt)
        //{
        //    var list = new List<Dictionary<string, object>>();

        //    foreach (DataRow row in dt.Rows)
        //    {
        //        var dict = new Dictionary<string, object>();

        //        foreach (DataColumn col in dt.Columns)
        //        {
        //            dict[col.ColumnName] = row[col];
        //        }
        //        list.Add(dict);
        //    }
        //    JavaScriptSerializer serializer = new JavaScriptSerializer();
        //    return serializer.Serialize(list);
        //}

        private static T GetItem<T>(DataRow dr)
        {
            Type temp = typeof(T);
            T obj = Activator.CreateInstance<T>();

            foreach (DataColumn column in dr.Table.Columns)
            {
                foreach (PropertyInfo pro in temp.GetProperties())
                {
                    try
                    {
                        if (pro.Name == column.ColumnName)
                            if (dr[column.ColumnName] != DBNull.Value)
                            {
                                pro.SetValue(obj, dr[column.ColumnName], null);
                            }
                            else
                                continue;
                    }
                    catch (Exception ex)
                    {

                    }
                }
            }
            return obj;
        }
        #endregion

    }
}
