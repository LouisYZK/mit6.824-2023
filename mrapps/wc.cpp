#include <dynamic.hpp>
#include <string>
#include <cctype>
#include <vector>
#include <sstream>



class WordCount: public MapReduceApp,
                    public AppFactory<MapReduceApp, WordCount>{
public:
    WordCount() {}
    std::vector<KeyValue> map(const std::string& filename, const std::string& contents) override {
        std::vector<std::string> words;
        std::stringstream wordss;
        for (auto& ch: contents) {
            if (std::isalpha(static_cast<unsigned char>(ch))) {
                wordss << ch;
            } else if (wordss.tellp() > 0) {
                words.push_back(wordss.str());
                wordss.str(std::string());
                wordss.clear();
            }
        }
        std::vector<KeyValue> kva;
        for (const auto& w : words) {
            kva.push_back({w, "1"});
        }
        return kva;
    }
    std::string reduce(const std::string& key, const std::vector<std::string>& values) override {
        return std::to_string(values.size());
    }
};