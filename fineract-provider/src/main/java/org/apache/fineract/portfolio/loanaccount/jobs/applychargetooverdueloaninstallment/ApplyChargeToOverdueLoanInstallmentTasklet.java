/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.fineract.portfolio.loanaccount.jobs.applychargetooverdueloaninstallment;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.fineract.infrastructure.codes.data.CodeValueData;
import org.apache.fineract.infrastructure.codes.service.CodeValueReadPlatformService;
import org.apache.fineract.infrastructure.configuration.domain.ConfigurationDomainService;
import org.apache.fineract.infrastructure.core.api.JsonCommand;
import org.apache.fineract.infrastructure.core.data.ApiParameterError;
import org.apache.fineract.infrastructure.core.exception.AbstractPlatformDomainRuleException;
import org.apache.fineract.infrastructure.core.exception.PlatformApiDataValidationException;
import org.apache.fineract.infrastructure.core.serialization.FromJsonHelper;
import org.apache.fineract.infrastructure.core.service.DateUtils;
import org.apache.fineract.infrastructure.jobs.exception.JobExecutionException;
import org.apache.fineract.portfolio.loanaccount.api.LoanApiConstants;
import org.apache.fineract.portfolio.loanaccount.loanschedule.data.OverdueLoanScheduleData;
import org.apache.fineract.portfolio.loanaccount.service.LoanChargeWritePlatformService;
import org.apache.fineract.portfolio.loanaccount.service.LoanReadPlatformService;
import org.apache.fineract.portfolio.loanaccount.service.LoanWritePlatformService;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@RequiredArgsConstructor
public class ApplyChargeToOverdueLoanInstallmentTasklet implements Tasklet {

    private final ConfigurationDomainService configurationDomainService;
    private final LoanReadPlatformService loanReadPlatformService;
    private final LoanChargeWritePlatformService loanChargeWritePlatformService;
    private final LoanWritePlatformService loanWritePlatformService;
    private final CodeValueReadPlatformService codeValueReadPlatformService;
    private final FromJsonHelper fromJsonHelper;
    private final long autoChargeOffOverdueDays;
    private final PlatformTransactionManager transactionManager;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        final Long penaltyWaitPeriodValue = configurationDomainService.retrievePenaltyWaitPeriod();
        final Boolean backdatePenalties = configurationDomainService.isBackdatePenaltiesEnabled();
        final Collection<OverdueLoanScheduleData> overdueLoanScheduledInstallments = loanReadPlatformService
                .retrieveAllLoansWithOverdueInstallments(penaltyWaitPeriodValue, backdatePenalties);

        if (!overdueLoanScheduledInstallments.isEmpty()) {
            final Map<Long, Collection<OverdueLoanScheduleData>> overdueScheduleData = new HashMap<>();
            for (final OverdueLoanScheduleData overdueInstallment : overdueLoanScheduledInstallments) {
                if (overdueScheduleData.containsKey(overdueInstallment.getLoanId())) {
                    overdueScheduleData.get(overdueInstallment.getLoanId()).add(overdueInstallment);
                } else {
                    Collection<OverdueLoanScheduleData> loanData = new ArrayList<>();
                    loanData.add(overdueInstallment);
                    overdueScheduleData.put(overdueInstallment.getLoanId(), loanData);
                }
            }

            final TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);
            txTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);

            List<Throwable> exceptions = new ArrayList<>();
            for (Map.Entry<Long, Collection<OverdueLoanScheduleData>> entry : overdueScheduleData.entrySet()) {
                try {
                    txTemplate.executeWithoutResult(status -> {
                        if (!entry.getValue().isEmpty()) {
                            if (autoChargeOffOverdueDays > 0 && shouldChargeOff(entry.getValue())) {
                                boolean chargedOff = tryChargeOffLoan(entry.getKey());
                                if (chargedOff) {
                                    return;
                                }
                            }
                            loanChargeWritePlatformService.applyOverdueChargesForLoan(entry.getKey(), entry.getValue());
                        }
                    });
                } catch (final PlatformApiDataValidationException e) {
                    final List<ApiParameterError> errors = e.getErrors();
                    for (final ApiParameterError error : errors) {
                        log.error("Apply Charges due for overdue loans failed for account {} with message: {}", entry.getKey(),
                                error.getDeveloperMessage(), e);
                    }
                    exceptions.add(e);
                } catch (final AbstractPlatformDomainRuleException e) {
                    log.error("Apply Charges due for overdue loans failed for account {} with message: {}", entry.getKey(),
                            e.getDefaultUserMessage(), e);
                    exceptions.add(e);
                } catch (Exception e) {
                    log.error("Apply Charges due for overdue loans failed for account {}", entry.getKey(), e);
                    exceptions.add(e);
                }
            }
            if (!exceptions.isEmpty()) {
                throw new JobExecutionException(exceptions);
            }
        }
        return RepeatStatus.FINISHED;
    }

    private boolean shouldChargeOff(Collection<OverdueLoanScheduleData> overdueInstallments) {
        final LocalDate businessDate = DateUtils.getBusinessLocalDate();
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        LocalDate earliestDueDate = null;
        for (OverdueLoanScheduleData installment : overdueInstallments) {
            LocalDate dueDate = LocalDate.parse(installment.getDueDate(), formatter);
            if (earliestDueDate == null || dueDate.isBefore(earliestDueDate)) {
                earliestDueDate = dueDate;
            }
        }
        if (earliestDueDate == null) {
            return false;
        }
        long overdueDays = ChronoUnit.DAYS.between(earliestDueDate, businessDate);
        return overdueDays >= autoChargeOffOverdueDays;
    }

    private boolean tryChargeOffLoan(Long loanId) {
        try {
            List<CodeValueData> chargeOffReasons = codeValueReadPlatformService
                    .retrieveCodeValuesByCode(LoanApiConstants.CHARGE_OFF_REASONS);
            if (chargeOffReasons == null || chargeOffReasons.isEmpty()) {
                return false;
            }
            CodeValueData firstReason = chargeOffReasons.stream().filter(CodeValueData::isActive).findFirst().orElse(null);
            if (firstReason == null) {
                return false;
            }

            final LocalDate transactionDate = DateUtils.getBusinessLocalDate();
            JsonObject json = new JsonObject();
            json.addProperty(LoanApiConstants.transactionDateParamName, transactionDate.format(DateTimeFormatter.ofPattern("dd MMMM yyyy")));
            json.addProperty(LoanApiConstants.dateFormatParameterName, "dd MMMM yyyy");
            json.addProperty(LoanApiConstants.localeParameterName, "en");
            json.addProperty(LoanApiConstants.chargeOffReasonIdParamName, firstReason.getId());

            final JsonElement parsedCommand = json;
            final JsonCommand command = JsonCommand.from(json.toString(), parsedCommand, fromJsonHelper, null, null, null, null, null,
                    loanId, null, null, null, null, null, null, null, null);

            loanWritePlatformService.chargeOff(command);
            log.info("Auto charged-off loan {} (overdue >= {} days, reason: {})", loanId, autoChargeOffOverdueDays,
                    firstReason.getName());
            return true;
        } catch (Exception e) {
            log.warn("Auto charge-off failed for loan {}: {}", loanId, e.getMessage());
            return false;
        }
    }
}
