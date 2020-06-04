statistics_proto_list = statistics_pb2.DatasetFeatureStatisticsList()
for folder in tf.io.gfile.listdir(drift_reports_path):
    statistics_proto = tfdv.load_statistics(
        '{}/{}{}'.format(drift_reports_path, folder, 'stats.pb'))
    new_stats_proto = statistics_proto_list.datasets.add()
    new_stats_proto.CopyFrom(statistics_proto.datasets[0])
    new_stats_proto.name = folder[:-1]

tfdv.write_stats_text(statistics_proto_list, aggregated_statistics_path)



class StatsSeries():
    def __init__(self, stats_proto_list):
        self._stats_proto_list = stats_proto_list

    def get_feature_means(self, feature_index):
        list_of_means = {
             dataset.name[0:16]: dataset.features[feature_index].num_stats.mean
             for dataset in self._stats_proto_list.datasets}
        
        return list_of_means
