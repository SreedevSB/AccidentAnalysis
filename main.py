import yaml
class AccidentAnalysis:
  def __init__(self, config_file):
    self.get_data(config_file)

  def read_yaml(file_path):
      """
      Read YAML Config file in YAML format
      :param file_path: path to config file
      :return: config details dict
      """
      with open(file_path, "r") as f:
          return yaml.safe_load(f)

  def get_data(self, config_file):
    input_data = self.read_yaml(config_file).get("INPUT_FILES")
    self.df_person = self.get_csv_data(input_data.get("Person"))
    self.df_units = self.get_csv_data(input_data.get("Units"))
    self.df_charges = self.get_csv_data(input_data.get("Charges"))
    self.df_endorses = self.get_csv_data(input_data.get("Endorses"))
    self.df_restrict = self.get_csv_data(input_data.get("Restrict"))

  def get_csv_data(self, path):
    return spark.read.options(header = True).csv(path)


  def get_num_crashes_with_male_people(self):
    return self.df_person.filter("PRSN_GNDR_ID = 'MALE'").select("CRASH_ID").distinct().count()

  def get_num_twowheelers_booked(self):
    return self.df_units.where(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE")).count()
  
  def get_state_with_max_females_involved(self):
    return self.df_person.filter("PRSN_GNDR_ID = 'FEMALE'")\
            .groupby("DRVR_LIC_STATE_ID")\
            .agg(countDistinct("CRASH_ID")\
            .alias("crash_count"))\
            .sort(col("crash_count")\
            .desc()).limit(1).collect()[0][0]

  def get_top15_vehicles_with_most_casualities(self):
    df_top15 = self.df_units.filter("VEH_MAKE_ID != 'NA'")\
              .withColumn("INJ_AND_DEATHS", expr("TOT_INJRY_CNT + DEATH_CNT"))\
              .groupby("VEH_MAKE_ID")\
              .agg(sum("INJ_AND_DEATHS").alias("INJ_AND_DEATHS_TOTAL"))\
              .sort(col("INJ_AND_DEATHS_TOTAL").desc())
    df_top5_to_15 = df_top15.limit(15).subtract(df_top15.limit(5))
    return [item[0] for item in df_top5_to_15.collect()]
      
  def get_top_ethnic_per_vehicle_type(self):
    df_join = self.df_person.join(df_units, on=["CRASH_ID"], how="inner")\
              .filter(~df_join["VEH_BODY_STYL_ID"].isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                            "OTHER  (EXPLAIN IN NARRATIVE)"]))\
              .filter(~df_join["PRSN_ETHNICITY_ID"].isin(["NA", "UNKNOWN"]))\
              .groupby("VEH_BODY_STYL_ID","PRSN_ETHNICITY_ID").count()
    df_partition = df_join.withColumn("row", row_number().over(
        Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()
      )
    )).filter("row = 1").drop("row").show()

  def get_top5_zipcodes_with_highest_num_crashes_with_alcohol_as_factor(self):
      return self.df_units.join(self.df_person, on=['CRASH_ID'], how='inner'). \
            dropna(subset=["DRVR_ZIP"]). \
            filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
      
  def get_crashid_with_no_damage(self):        
    df = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner'). \
            filter(
            (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            )
        ). \
            filter(self.df_damages.DAMAGED_PROPERTY == "NONE"). \
            filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
    


  def get_top5_vehicle_brand(self):      
      top_25_state_list = [row[0] for row in self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
          groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
      top_10_used_vehicle_colors = [row[0] for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA").
          groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

      df = self.df_charges.join(self.df_person, on=['CRASH_ID'], how='inner'). \
          join(self.df_units, on=['CRASH_ID'], how='inner'). \
          filter(self.df_charges.CHARGE.contains("SPEED")). \
          filter(self.df_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
          filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
          filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
          groupby("VEH_MAKE_ID").count(). \
          orderBy(col("count").desc()).limit(5)

          
