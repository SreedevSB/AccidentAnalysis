import yaml
class Utils:
  def read_yaml(self, file_path):
      """
      Read YAML Config file in YAML format
      :param file_path: path to config file
      :return: config details dict
      """
      with open(file_path, "r") as f:
          return yaml.safe_load(f)
