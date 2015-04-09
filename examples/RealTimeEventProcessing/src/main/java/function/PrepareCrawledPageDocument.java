package function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class PrepareCrawledPageDocument extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String url = tridentTuple.getString(0);
        String content_html = tridentTuple.getString(1);
        String title = tridentTuple.getString(2);


        String source = "{\"url\":\""+url+"\", \"content\":\""+"content_ignored_json_format_TODO"+"\", \"title\":\""+title+"\"}";
        System.out.println("----- PrepareCrawledPageDocument: id = "+url);
        System.out.println("----- PrepareCrawledPageDocument: source = "+source);

        tridentCollector.emit(new Values("crawl_index", "crawl_type", url, source ));
    }
}