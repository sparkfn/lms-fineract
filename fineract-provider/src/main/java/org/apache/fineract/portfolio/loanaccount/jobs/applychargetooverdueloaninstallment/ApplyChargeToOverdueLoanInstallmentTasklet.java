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
import java.math.BigDecimal;
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
import org.apache.fineract.infrastructure.jobs.domain.JobParameter;
import org.apache.fineract.infrastructure.jobs.domain.JobParameterRepository;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Slf4j
@RequiredArgsConstructor
public class ApplyChargeToOverdueLoanInstallmentTasklet implements Tasklet {

    private static final long PENALTY_JOB_ID = 12L;

    private final ConfigurationDomainService configurationDomainService;
    private final LoanReadPlatformService loanReadPlatformService;
    private final LoanChargeWritePlatformService loanChargeWritePlatformService;
    private final LoanWritePlatformService loanWritePlatformService;
    private final CodeValueReadPlatformService codeValueReadPlatformService;
    private final FromJsonHelper fromJsonHelper;
    private final JobParameterRepository jobParameterRepository;
    private final PlatformTransactionManager transactionManager;
    private final JdbcTemplate jdbcTemplate;

    private String getJobParameter(String name, String defaultValue) {
        List<JobParameter> params = jobParameterRepository.findJobParametersByJobId(PENALTY_JOB_ID);
        for (JobParameter p : params) {
            if (name.equals(p.getParameterName())) {
                return p.getParameterValue();
            }
        }
        return defaultValue;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        final boolean autoChargeOffEnabled = Boolean.parseBoolean(getJobParameter("auto-chargeoff-enabled", "false"));
        final long autoChargeOffOverdueDays = Long.parseLong(getJobParameter("auto-chargeoff-overdue-days", "180"));
        final boolean linearPenaltyEnabled = Boolean.parseBoolean(getJobParameter("linear-penalty-enabled", "false"));

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
                    if (entry.getValue().isEmpty()) {
                        continue;
                    }

                    // Skip loans that are already charged off
                    if (isAlreadyChargedOff(entry.getKey())) {
                        continue;
                    }

                    // Check if loan qualifies for auto charge-off
                    if (autoChargeOffEnabled && autoChargeOffOverdueDays > 0 && shouldChargeOff(entry.getValue(), autoChargeOffOverdueDays)) {
                        // Try JPA charge-off in its own transaction
                        boolean chargedOff = false;
                        try {
                            txTemplate.executeWithoutResult(status -> {
                                chargeOffLoanViaService(entry.getKey(), autoChargeOffOverdueDays);
                            });
                            chargedOff = true;
                        } catch (Exception e) {
                            log.warn("Auto charge-off via service failed for loan {}, attempting SQL fallback: {}",
                                    entry.getKey(), e.getMessage());
                            // Try SQL fallback in a separate clean transaction
                            try {
                                txTemplate.executeWithoutResult(status -> {
                                    doFallbackChargeOffViaSql(entry.getKey(), autoChargeOffOverdueDays);
                                });
                                chargedOff = true;
                            } catch (Exception e2) {
                                log.error("SQL fallback charge-off also failed for loan {}: {}",
                                        entry.getKey(), e2.getMessage());
                            }
                        }
                        if (chargedOff) {
                            continue;
                        }
                    }

                    // Apply penalty charges in its own transaction
                    final Collection<OverdueLoanScheduleData> installmentsToCharge = linearPenaltyEnabled
                            ? findEarliestPerCharge(entry.getValue()) : entry.getValue();
                    txTemplate.executeWithoutResult(status -> {
                        loanChargeWritePlatformService.applyOverdueChargesForLoan(entry.getKey(), installmentsToCharge);
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

    private boolean isAlreadyChargedOff(Long loanId) {
        Boolean chargedOff = jdbcTemplate.queryForObject(
                "SELECT is_charged_off FROM m_loan WHERE id = ?", Boolean.class, loanId);
        return Boolean.TRUE.equals(chargedOff);
    }

    private boolean shouldChargeOff(Collection<OverdueLoanScheduleData> overdueInstallments, long autoChargeOffOverdueDays) {
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

    private Collection<OverdueLoanScheduleData> findEarliestPerCharge(Collection<OverdueLoanScheduleData> installments) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        final Map<Long, OverdueLoanScheduleData> earliestByCharge = new HashMap<>();
        for (OverdueLoanScheduleData installment : installments) {
            Long chargeId = installment.getChargeId();
            OverdueLoanScheduleData current = earliestByCharge.get(chargeId);
            if (current == null) {
                earliestByCharge.put(chargeId, installment);
            } else {
                LocalDate currentDue = LocalDate.parse(current.getDueDate(), formatter);
                LocalDate candidateDue = LocalDate.parse(installment.getDueDate(), formatter);
                if (candidateDue.isBefore(currentDue)) {
                    earliestByCharge.put(chargeId, installment);
                }
            }
        }
        return earliestByCharge.values();
    }

    private Long getChargeOffReasonId() {
        List<CodeValueData> chargeOffReasons = codeValueReadPlatformService
                .retrieveCodeValuesByCode(LoanApiConstants.CHARGE_OFF_REASONS);
        if (chargeOffReasons == null || chargeOffReasons.isEmpty()) {
            return null;
        }
        return chargeOffReasons.stream().filter(CodeValueData::isActive).findFirst().map(CodeValueData::getId).orElse(null);
    }

    private void chargeOffLoanViaService(Long loanId, long autoChargeOffOverdueDays) {
        Long reasonId = getChargeOffReasonId();
        if (reasonId == null) {
            throw new RuntimeException("No active charge-off reason found");
        }

        final LocalDate transactionDate = DateUtils.getBusinessLocalDate();
        JsonObject json = new JsonObject();
        json.addProperty(LoanApiConstants.transactionDateParamName, transactionDate.format(DateTimeFormatter.ofPattern("dd MMMM yyyy")));
        json.addProperty(LoanApiConstants.dateFormatParameterName, "dd MMMM yyyy");
        json.addProperty(LoanApiConstants.localeParameterName, "en");
        json.addProperty(LoanApiConstants.chargeOffReasonIdParamName, reasonId);

        final JsonElement parsedCommand = json;
        final JsonCommand command = JsonCommand.from(json.toString(), parsedCommand, fromJsonHelper, null, null, null, null, null,
                loanId, null, null, null, null, null, null, null, null);

        loanWritePlatformService.chargeOff(command);
        log.info("Auto charged-off loan {} (overdue >= {} days)", loanId, autoChargeOffOverdueDays);
    }

    private void doFallbackChargeOffViaSql(Long loanId, long autoChargeOffOverdueDays) {
        Long reasonId = getChargeOffReasonId();
        if (reasonId == null) {
            throw new RuntimeException("No active charge-off reason found");
        }

        final LocalDate transactionDate = DateUtils.getBusinessLocalDate();

        Map<String, Object> loanData = jdbcTemplate.queryForMap(
                "SELECT mc.office_id, ml.principal_outstanding_derived, ml.interest_outstanding_derived, "
                        + "ml.fee_charges_outstanding_derived, ml.penalty_charges_outstanding_derived, ml.total_outstanding_derived "
                        + "FROM m_loan ml JOIN m_client mc ON ml.client_id = mc.id WHERE ml.id = ?",
                loanId);

        Long officeId = ((Number) loanData.get("office_id")).longValue();
        BigDecimal principal = (BigDecimal) loanData.get("principal_outstanding_derived");
        BigDecimal interest = (BigDecimal) loanData.get("interest_outstanding_derived");
        BigDecimal fees = (BigDecimal) loanData.get("fee_charges_outstanding_derived");
        BigDecimal penalties = (BigDecimal) loanData.get("penalty_charges_outstanding_derived");
        BigDecimal total = (BigDecimal) loanData.get("total_outstanding_derived");

        jdbcTemplate.update(
                "INSERT INTO m_loan_transaction (loan_id, office_id, is_reversed, transaction_type_enum, transaction_date, "
                        + "amount, principal_portion_derived, interest_portion_derived, fee_charges_portion_derived, "
                        + "penalty_charges_portion_derived, submitted_on_date, manually_adjusted_or_reversed, "
                        + "created_by, last_modified_by, created_on_utc, last_modified_on_utc, version) "
                        + "VALUES (?, ?, 0, 27, ?, ?, ?, ?, ?, ?, ?, 0, 2, 2, NOW(6), NOW(6), 1)",
                loanId, officeId, transactionDate, total, principal, interest, fees, penalties, transactionDate);

        jdbcTemplate.update(
                "UPDATE m_loan SET is_charged_off = 1, charged_off_on_date = ?, charge_off_reason_cv_id = ?, charged_off_by_userid = 2 "
                        + "WHERE id = ?",
                transactionDate, reasonId, loanId);

        log.info("Auto charged-off loan {} via SQL fallback (overdue >= {} days)", loanId, autoChargeOffOverdueDays);
    }
}
