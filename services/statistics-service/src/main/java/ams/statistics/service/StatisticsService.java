package ams.statistics.service;

import ams.statistics.jpa.StatisticalModelData;
import ams.statistics.jpa.StatisticalModelRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class StatisticsService {
    private final StatisticalModelRepository statisticalModelRepository;

    @Transactional(rollbackFor = Exception.class)
    public void saveBatch(List<StatisticalModelData> batch){
        statisticalModelRepository.saveAll(batch);
    }
}
